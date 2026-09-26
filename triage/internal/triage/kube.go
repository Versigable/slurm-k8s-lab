package triage

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/kube"
)

// Schedulers a recommendation can be about.
const (
	SchedulerSlurm      = "slurm"
	SchedulerKubernetes = "kubernetes"
)

// Categories that only occur on Kubernetes nodes.
const (
	CategoryKubelet          Category = "kubelet"           // kubelet up but reporting NotReady
	CategoryResourcePressure Category = "resource_pressure" // Memory/Disk/PID pressure
	CategoryKernelEvents     Category = "kernel_events"     // repeated NPD kernel events on a Ready node
)

// Kubernetes adds DefaultTolerationSeconds (300 s) for the not-ready and
// unreachable NoExecute taints to pods that don't set their own.
const defaultEvictionDelay = 300 * time.Second

// node-problem-detector's temporary kernel events (kernel-monitor.json).
var npdKernelEvents = []string{"TaskHung", "KernelOops", "OOMKilling", "MemoryReadError", "UnregisterNetDevice"}

// KubeSnapshot is the Kubernetes state triage reads, taken at one instant.
type KubeSnapshot struct {
	Now    time.Time
	Nodes  []kube.Node
	Pods   []kube.Pod
	Events []kube.Event // events whose involvedObject is a Node
	PDBs   []kube.PDB
}

// AssessKube returns the recommendation for one Kubernetes node.
func AssessKube(s KubeSnapshot, name string, p Policy) (Recommendation, error) {
	p = p.withDefaults()
	i := slices.IndexFunc(s.Nodes, func(n kube.Node) bool { return n.Metadata.Name == name })
	if i < 0 {
		return Recommendation{}, fmt.Errorf("node %q not found", name)
	}
	a := kubeAssessor{s: s, p: p, n: s.Nodes[i]}
	return a.assess(), nil
}

// AssessKubeAll assesses every Kubernetes node, most severe first.
func AssessKubeAll(s KubeSnapshot, p Policy) []Recommendation {
	recs := make([]Recommendation, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		r, _ := AssessKube(s, n.Metadata.Name, p)
		recs = append(recs, r)
	}
	SortRecommendations(recs)
	return recs
}

// SortRecommendations orders by severity (critical first), then node name.
func SortRecommendations(recs []Recommendation) {
	slices.SortStableFunc(recs, func(a, b Recommendation) int {
		if c := cmp.Compare(b.Severity.rank(), a.Severity.rank()); c != 0 {
			return c
		}
		return cmp.Compare(a.Node, b.Node)
	})
}

type kubeAssessor struct {
	s KubeSnapshot
	p Policy
	n kube.Node
}

func (a kubeAssessor) assess() Recommendation {
	n := a.n
	name := n.Metadata.Name
	reason := n.Metadata.Annotations[kube.ReasonAnnotation]
	r := Recommendation{Node: name, Scheduler: SchedulerKubernetes, Category: CategoryNone}
	r.Evidence = append(r.Evidence, a.stateLine())
	if reason != "" {
		r.Evidence = append(r.Evidence, fmt.Sprintf("cordon reason %q (annotation %s)", reason, kube.ReasonAnnotation))
	}
	workload := a.workloadPods()
	events, eventCount := a.kernelEvents()
	if eventCount > 0 {
		r.Evidence = append(r.Evidence, fmt.Sprintf("%d node-problem-detector kernel event(s) recently: %s", eventCount, eventList(events)))
	}

	ready, _ := n.Condition("Ready")
	switch {
	case a.conditionTrue("ReadonlyFilesystem"):
		a.readonlyFS(&r, workload)
	case ready.Status == "Unknown":
		a.unreachable(&r, ready, workload)
	case ready.Status == "False":
		r.Action, r.Severity, r.Category = ActionInvestigate, SeverityCritical, CategoryKubelet
		r.Summary = fmt.Sprintf("The kubelet is up but reports NotReady: %s", firstNonEmpty(ready.Message, ready.Reason))
		r.NextSteps = []string{"Read the kubelet's message and journal; runtime, CNI and certificate problems are the usual causes."}
		r.Commands = []string{
			fmt.Sprintf("kubectl describe node %s", name),
			fmt.Sprintf("ssh %s 'journalctl -u kubelet -n 50; systemctl status containerd'", name),
		}
	case a.conditionTrue("KernelDeadlock"):
		c, _ := n.Condition("KernelDeadlock")
		r.Action, r.Severity, r.Category = ActionInvestigate, SeverityCritical, CategoryStuckProcess
		r.Summary = "A task is hung in the kernel. It won't recover on its own; cordon, drain and reboot."
		r.Evidence = append(r.Evidence, fmt.Sprintf("KernelDeadlock=True (%s) since %s: %s", c.Reason, a.since(c.LastTransitionTime), c.Message))
		r.NextSteps = []string{"Cordon so nothing new lands.", "Drain what can move.", "Reboot; look for processes in D state first."}
		r.Commands = []string{
			fmt.Sprintf("kubectl cordon %s", name),
			fmt.Sprintf("kubectl drain %s --ignore-daemonsets --delete-emptydir-data --timeout=5m", name),
			fmt.Sprintf("ssh %s \"ps -eo pid,stat,wchan:32,cmd | awk '\\$2 ~ /D/'\"", name),
		}
	case len(a.pressures()) > 0:
		r.Action, r.Severity, r.Category = ActionInvestigate, SeverityWarning, CategoryResourcePressure
		r.Summary = fmt.Sprintf("Under %s; the kubelet is evicting or refusing pods.", strings.Join(a.pressures(), ", "))
		r.NextSteps = []string{"Find what's consuming the resource; the kubelet evicts lowest-priority pods first."}
		r.Commands = []string{fmt.Sprintf("kubectl describe node %s", name), fmt.Sprintf("ssh %s 'df -h; free -m'", name)}
	case n.Spec.Unschedulable:
		a.cordoned(&r, reason, workload)
	case eventCount >= a.p.KubeEventThreshold:
		r.Action, r.Severity, r.Category = ActionInvestigate, SeverityWarning, CategoryKernelEvents
		r.Summary = fmt.Sprintf("Ready, but the kernel reported %d problem event(s). Look before it becomes a deadlock or an outage.", eventCount)
		r.NextSteps = []string{"Read the kernel log around the event times.", "Cordon if it keeps happening."}
		r.Commands = []string{fmt.Sprintf("ssh %s 'dmesg -T | tail -100'", name), fmt.Sprintf("kubectl cordon %s", name)}
	default:
		r.Action, r.Severity = ActionNone, SeverityInfo
		r.Summary = "Healthy."
		if eventCount > 0 {
			r.Summary = fmt.Sprintf("Healthy; %d kernel event(s) is below the threshold of %d.", eventCount, a.p.KubeEventThreshold)
		}
	}
	return r
}

func (a kubeAssessor) readonlyFS(r *Recommendation, workload []kube.Pod) {
	name := a.n.Metadata.Name
	c, _ := a.n.Condition("ReadonlyFilesystem")
	r.Action, r.Severity, r.Category = ActionEscalateHardware, SeverityCritical, CategoryHardware
	r.Summary = "The kernel remounted a filesystem read-only, which usually means a disk or filesystem error. Take the node out."
	r.Evidence = append(r.Evidence, fmt.Sprintf("ReadonlyFilesystem=True (%s) since %s: %s", c.Reason, a.since(c.LastTransitionTime), c.Message))
	if len(workload) > 0 {
		r.Evidence = append(r.Evidence, fmt.Sprintf("%d workload pod(s) still on the node: %s", len(workload), podList(workload, 5)))
	}
	r.NextSteps = []string{
		"Cordon, then drain; pods writing to local disk are already failing.",
		"Check the kernel log for I/O errors and the disk's SMART data before any reboot.",
		"Hardware ticket if the disk is failing; fsck and reboot only if it isn't.",
	}
	r.Commands = []string{
		fmt.Sprintf("kubectl cordon %s", name),
		fmt.Sprintf("kubectl drain %s --ignore-daemonsets --delete-emptydir-data --timeout=5m", name),
		fmt.Sprintf("ssh %s \"dmesg -T | grep -iE 'I/O error|remount|EXT4-fs error'\"", name),
	}
}

func (a kubeAssessor) unreachable(r *Recommendation, ready kube.Condition, workload []kube.Pod) {
	name := a.n.Metadata.Name
	r.Action, r.Severity, r.Category = ActionInvestigate, SeverityCritical, CategoryUnreachable
	// Not lastHeartbeatTime: kubelets only rewrite node status on change or every
	// 5 minutes, so it can look minutes stale on a healthy node. Liveness is the
	// kubelet's Lease; the node controller flips Ready to Unknown when it stops renewing.
	r.Evidence = append(r.Evidence, fmt.Sprintf("Ready=Unknown since %s: the kubelet's node lease stopped renewing", a.since(ready.LastTransitionTime)))
	taint, tainted := a.n.Taint("node.kubernetes.io/unreachable", "NoExecute")
	switch {
	case tainted && taint.TimeAdded != nil && len(workload) > 0:
		evictAt := taint.TimeAdded.Add(defaultEvictionDelay)
		if a.s.Now.Before(evictAt) {
			r.Summary = fmt.Sprintf("The kubelet stopped reporting. %d workload pod(s) will be evicted in %s unless it comes back.",
				len(workload), evictAt.Sub(a.s.Now).Round(time.Second))
		} else {
			r.Summary = fmt.Sprintf("The kubelet stopped reporting and the %s eviction window has passed; %d pod(s) are being evicted.",
				defaultEvictionDelay, len(workload))
		}
		r.Evidence = append(r.Evidence, fmt.Sprintf("unreachable:NoExecute taint added %s ago; pods without their own toleration are evicted after %s",
			a.age(*taint.TimeAdded), defaultEvictionDelay))
		r.Evidence = append(r.Evidence, fmt.Sprintf("workload pods on the node: %s", podList(workload, 5)))
	default:
		r.Summary = "The kubelet stopped reporting. Find out whether the host or just the kubelet is down."
	}
	r.NextSteps = []string{
		"Check the host: power, console, network.",
		"If it's up, restart the kubelet (and containerd if it's wedged). Running containers survive a kubelet restart.",
		"If the host is gone, let the eviction happen and follow up on the hardware.",
	}
	r.Commands = []string{
		fmt.Sprintf("ping -c3 %s", name),
		fmt.Sprintf("ssh %s 'sudo systemctl restart kubelet && journalctl -u kubelet -n 20'", name),
	}
}

func (a kubeAssessor) cordoned(r *Recommendation, reason string, workload []kube.Pod) {
	name := a.n.Metadata.Name
	r.Category = Classify(reason)
	if r.Category == CategoryNone {
		r.Category = CategoryOther
	}
	if len(workload) > 0 {
		blocking := a.blockingPDBs(workload)
		if len(blocking) > 0 {
			r.Action, r.Severity = ActionInvestigate, SeverityWarning
			r.Summary = fmt.Sprintf("Cordoned, but its drain is blocked: %d PodDisruptionBudget(s) allow no disruptions.", len(blocking))
			for _, b := range blocking {
				r.Evidence = append(r.Evidence, b)
			}
			r.NextSteps = []string{
				"A drain here would hang. Either add capacity elsewhere so the PDB's pods can reschedule, or agree a disruption with the owner.",
				"Don't delete pods by hand to get past a PDB; that's the outage the PDB exists to prevent.",
			}
			r.Commands = []string{
				"kubectl get pdb -A",
				fmt.Sprintf("kubectl drain %s --ignore-daemonsets --delete-emptydir-data --timeout=2m   # will wait on the PDB", name),
			}
			return
		}
		r.Action, r.Severity = ActionDrain, SeverityInfo
		r.Summary = fmt.Sprintf("Cordoned with %d workload pod(s) still on it and nothing blocking eviction. Finish the drain.", len(workload))
		r.Evidence = append(r.Evidence, fmt.Sprintf("workload pods: %s", podList(workload, 5)))
		r.Commands = []string{fmt.Sprintf("kubectl drain %s --ignore-daemonsets --delete-emptydir-data --timeout=5m", name)}
		return
	}
	if r.Category == CategoryMaintenance {
		r.Action, r.Severity = ActionResume, SeverityInfo
		r.Summary = "Cordoned for maintenance and empty. Uncordon once the work is done."
		r.Preconditions = []string{fmt.Sprintf("The maintenance in %q is complete and verified.", reason)}
		r.Commands = []string{fmt.Sprintf("kubectl uncordon %s", name)}
		return
	}
	r.Action, r.Severity = ActionInvestigate, SeverityWarning
	r.Summary = "Cordoned and empty, with no recognized reason. Find out who cordoned it before uncordoning."
}

// blockingPDBs describes PDBs that allow zero disruptions and cover a pod on the node.
func (a kubeAssessor) blockingPDBs(workload []kube.Pod) []string {
	var out []string
	for _, pdb := range a.s.PDBs {
		if pdb.Status.DisruptionsAllowed > 0 {
			continue
		}
		var covered []kube.Pod
		for _, p := range workload {
			if pdb.Covers(p) {
				covered = append(covered, p)
			}
		}
		if len(covered) > 0 {
			out = append(out, fmt.Sprintf("PDB %s allows 0 disruptions (healthy %d/%d desired) and covers %s on this node",
				pdb.Ref(), pdb.Status.CurrentHealthy, pdb.Status.DesiredHealthy, podList(covered, 5)))
		}
	}
	return out
}

func (a kubeAssessor) workloadPods() []kube.Pod {
	var out []kube.Pod
	for _, p := range a.s.Pods {
		if p.Spec.NodeName == a.n.Metadata.Name && p.Workload() {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(x, y kube.Pod) int { return cmp.Compare(x.Ref(), y.Ref()) })
	return out
}

func (a kubeAssessor) kernelEvents() ([]kube.Event, int) {
	var out []kube.Event
	total := 0
	for _, e := range a.s.Events {
		if e.InvolvedObject.Name == a.n.Metadata.Name && e.Type == "Warning" && slices.Contains(npdKernelEvents, e.Reason) {
			out = append(out, e)
			total += e.Occurrences()
		}
	}
	slices.SortFunc(out, func(x, y kube.Event) int { return x.When().Compare(y.When()) })
	return out, total
}

func (a kubeAssessor) conditionTrue(t string) bool {
	c, ok := a.n.Condition(t)
	return ok && c.Status == "True"
}

func (a kubeAssessor) pressures() []string {
	var out []string
	for _, t := range []string{"MemoryPressure", "DiskPressure", "PIDPressure"} {
		if a.conditionTrue(t) {
			out = append(out, t)
		}
	}
	return out
}

func (a kubeAssessor) stateLine() string {
	n := a.n
	ready, _ := n.Condition("Ready")
	parts := []string{"Ready=" + firstNonEmpty(ready.Status, "missing")}
	if n.Spec.Unschedulable {
		parts = append(parts, "cordoned")
	}
	for _, c := range n.Status.Conditions {
		if c.Type != "Ready" && c.Status == "True" {
			parts = append(parts, c.Type+"=True")
		}
	}
	line := strings.Join(parts, ", ")
	if len(n.Spec.Taints) > 0 {
		var ts []string
		for _, t := range n.Spec.Taints {
			ts = append(ts, t.Key+":"+t.Effect)
		}
		line += "; taints " + strings.Join(ts, ", ")
	}
	return line + "; kubelet " + n.Status.NodeInfo.KubeletVersion
}

func (a kubeAssessor) since(t time.Time) string {
	if t.IsZero() || a.s.Now.IsZero() {
		return "an unknown time"
	}
	return a.age(t) + " ago"
}

func (a kubeAssessor) age(t time.Time) string {
	if t.IsZero() || a.s.Now.IsZero() {
		return "an unknown time"
	}
	return max(a.s.Now.Sub(t), 0).Round(time.Second).String()
}

func podList(pods []kube.Pod, limit int) string {
	var names []string
	for i, p := range pods {
		if i == limit {
			names = append(names, fmt.Sprintf("and %d more", len(pods)-limit))
			break
		}
		names = append(names, p.Ref())
	}
	return strings.Join(names, ", ")
}

func eventList(events []kube.Event) string {
	counts := map[string]int{}
	var order []string
	for _, e := range events {
		if _, seen := counts[e.Reason]; !seen {
			order = append(order, e.Reason)
		}
		counts[e.Reason] += e.Occurrences()
	}
	var parts []string
	for _, reason := range order {
		parts = append(parts, fmt.Sprintf("%s×%d", reason, counts[reason]))
	}
	return strings.Join(parts, ", ")
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}
