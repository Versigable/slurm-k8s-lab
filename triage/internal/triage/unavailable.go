package triage

import "fmt"

// ComponentUnavailable is the recommendation for a component triage couldn't
// read: a cluster's scheduler (slurmctld, or slurmrestd for Slinky), its
// accounting (slurmdbd), or the Kubernetes API. It goes in the same ranked list
// as node problems, says what still works, and what triage can't check meanwhile.
//
// Found by drills/: with slurmdbd or slurmctld down, triage used to return one
// error for every cluster, including the ones that were fine.
//
// k, when set, is the Kubernetes view: for Slinky, it says which node the
// Slurm control-plane pods run on, and whether that node is the real problem.
func ComponentUnavailable(cluster, component, errText string, k *KubeSnapshot) Recommendation {
	r := Recommendation{
		Node: component, Scheduler: SchedulerSlurm, Cluster: cluster,
		Action: ActionInvestigate, Severity: SeverityCritical, Category: CategoryControlPlane,
		Evidence: []string{fmt.Sprintf("%s didn't answer: %s", component, errText)},
	}
	switch component {
	case "slurmdbd":
		r.Severity, r.Category = SeverityWarning, CategoryAccounting
		r.Summary = "Accounting (slurmdbd) is unreachable. Scheduling continues and slurmctld queues job records until it's back; meanwhile triage can't check job history or node events."
		r.Evidence = append(r.Evidence, "not checked until it's back: NODE_FAIL counts per node, chronic drain/down history")
		r.NextSteps = []string{
			"Check slurmdbd and its database on the accounting host.",
			"Watch slurmctld's queue of unsent records drain once slurmdbd is back. Records are dropped if the queue fills (MaxDBDMsgs).",
		}
		r.Commands = []string{
			"sdiag | grep 'DBD Agent'   # records waiting for slurmdbd",
			"systemctl status slurmdbd mariadb   # on the accounting host",
		}
	case "slurmrestd":
		r.Summary = fmt.Sprintf("Can't reach slurmrestd for cluster %s. The cluster may be fine; triage just can't see it.", cluster)
		r.NextSteps = []string{
			"Check the slurmrestd and slurmctld pods and the Service in front of them.",
			"If they sit on a NotReady Kubernetes node, work that node (triage_node on it).",
		}
		r.Commands = []string{"kubectl -n slurm get pods -o wide", "kubectl -n slurm logs deploy/slurm-restapi --tail=50"}
		if node, kr, ok := slurmControlPlaneNode(k); ok {
			// Found by drills/: k8s-kubelet-stop. With the kubelet gone, Kubernetes
			// marks the node's pods NotReady and drops them from their Service:
			// slurmrestd vanishes although its container is still running.
			r.Summary = fmt.Sprintf("Can't reach slurmrestd for cluster %s: its control-plane pods run on Kubernetes node %s, which is in trouble. Fix that node first.", cluster, node)
			r.Evidence = append(r.Evidence, fmt.Sprintf("Kubernetes node %s: %s/%s: %s", node, kr.Action, kr.Severity, kr.Summary))
			r.NextSteps = []string{
				fmt.Sprintf("Work Kubernetes node %s (triage_node %s). Slurm itself may still be running there.", node, node),
				"Pods on an unreachable node are marked NotReady and dropped from their Service within a minute, so slurmrestd is unreachable even while its container runs.",
			}
		}
	case "kube-apiserver":
		r.Scheduler = SchedulerKubernetes
		r.Summary = "The Kubernetes API is unreachable. Running pods keep running, but nothing is scheduled, evicted or healed until it's back, and triage can't see node health."
		r.NextSteps = []string{
			"Check kube-apiserver and etcd on the control plane.",
			"If clients fail TLS, check certificate expiry (kubeadm certs check-expiration).",
		}
		r.Commands = []string{"kubectl get --raw '/readyz?verbose'", "sudo crictl ps --name 'kube-apiserver|etcd'   # on the control plane"}
	default: // slurmctld
		r.Summary = fmt.Sprintf("Can't get node and job state for cluster %s from %s. Running jobs keep running, but nothing new starts and node health can't be checked until it's back.", cluster, component)
		r.NextSteps = []string{
			"Check slurmctld on the controller: is it running, and what did it log last?",
			"Don't kill running jobs: they don't need slurmctld, and a restarted slurmctld recovers them from StateSaveLocation.",
			"If it won't start, check the StateSaveLocation disk and slurm.conf on the controller.",
		}
		r.Commands = []string{"scontrol ping", "systemctl status slurmctld; journalctl -u slurmctld -n 50   # on the controller"}
	}
	return r
}

// slurmControlPlaneNode finds a Kubernetes node with a fault (not planned
// work) that hosts a Slinky controller or slurmrestd pod.
func slurmControlPlaneNode(k *KubeSnapshot) (string, Recommendation, bool) {
	if k == nil {
		return "", Recommendation{}, false
	}
	for _, p := range k.Pods {
		c := p.Metadata.Labels["app.kubernetes.io/component"]
		if (c != "controller" && c != "restapi") || p.Metadata.Labels["app.kubernetes.io/part-of"] != "slurm" || p.Spec.NodeName == "" {
			continue
		}
		if kr, err := AssessKube(*k, p.Spec.NodeName, Policy{}); err == nil && nodeFault(kr.Category) {
			return p.Spec.NodeName, kr, true
		}
	}
	return "", Recommendation{}, false
}
