package triage

import (
	"strings"
	"testing"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
)

// slinkyFixture loads a Slinky scenario: the Slurm view (captured from
// slurmctld inside the cluster) linked to the Kubernetes view captured with it.
func slinkyFixture(t *testing.T, scenario string, withKube bool) Snapshot {
	t.Helper()
	s := fixtureSnapshot(t, scenario)
	s.Cluster = "slinky"
	if withKube {
		k := kubeFixture(t, scenario)
		s.Kube = &k
	}
	return s
}

func TestSlinkyHealthyNodesAreLinkedToTheirPods(t *testing.T) {
	s := slinkyFixture(t, "slinky-healthy", true)
	for _, r := range AssessAll(s, Policy{}) {
		if r.Action != ActionNone || r.Cluster != "slinky" {
			t.Errorf("%s: %s in cluster %q", r.Node, r.Action, r.Cluster)
		}
	}
	r := mustAssess(t, s, "slinky-0")
	if !contains(r.Evidence, "runs as pod slurm/slurm-worker-slinky-0 on Kubernetes node k8s-w1") {
		t.Errorf("want the pod link from the operator's node comment: %v", r.Evidence)
	}
}

func TestSlinkyOperatorDrainFollowsTheKubernetesNode(t *testing.T) {
	// Captured: read-only fs on k8s-w1, k8s-w1 cordoned, operator drained
	// slinky-0 with "slurm-operator: (FilesystemIsReadOnly: ...)".
	s := slinkyFixture(t, "slinky-cordoned-readonly", true)
	r := mustAssess(t, s, "slinky-0")
	// The Kubernetes node's hardware fault drives the Slurm node's recommendation.
	expect(t, r, ActionEscalateHardware, SeverityCritical, CategoryKubernetesManaged)
	if !contains(r.Evidence, "Kubernetes node k8s-w1: escalate_hardware/critical") {
		t.Errorf("want the Kubernetes node's assessment in evidence: %v", r.Evidence)
	}
	if !contains(r.NextSteps, "Don't resume this node in Slurm") || !contains(r.Commands, "kubectl uncordon k8s-w1") {
		t.Errorf("the fix belongs on the Kubernetes side: %v / %v", r.NextSteps, r.Commands)
	}
	if other := mustAssess(t, s, "slinky-1"); other.Action != ActionNone {
		t.Errorf("slinky-1 is on a healthy node: %s", other.Action)
	}
}

func TestSlinkyOperatorDrainWithoutKubernetesAccess(t *testing.T) {
	r := mustAssess(t, slinkyFixture(t, "slinky-cordoned-readonly", false), "slinky-0")
	expect(t, r, ActionInvestigate, SeverityWarning, CategoryKubernetesManaged)
}

func TestKubernetesNodeListsItsSlinkyNodes(t *testing.T) {
	k := kubeFixture(t, "slinky-cordoned-readonly")
	k.SlurmNodes = []SlurmNodeRef{{Name: "slinky-0", Cluster: "slinky", State: []string{"IDLE", "DRAIN"}, Reason: "slurm-operator: (x)", KubeNode: "k8s-w1"}}
	r := mustAssessKube(t, k, "k8s-w1")
	if !contains(r.Evidence, "hosts Slinky Slurm node slinky-0") {
		t.Errorf("evidence: %v", r.Evidence)
	}
}

func TestSlinkyOperatorDrainAfterKubernetesRecovers(t *testing.T) {
	// Kubernetes node already healthy and uncordoned; the operator just hasn't
	// undrained yet. Nothing to do but wait a few seconds.
	s := slinkyFixture(t, "slinky-cordoned-readonly", true)
	healthy := kubeFixture(t, "slinky-healthy")
	s.Kube = &healthy
	r := mustAssess(t, s, "slinky-0")
	if r.Action != ActionWait || r.Severity != SeverityInfo {
		t.Fatalf("got %s/%s, want wait/info", r.Action, r.Severity)
	}
}

func TestSlinkyPodParsing(t *testing.T) {
	n := slurm.Node{Comment: `{"namespace":"slurm","podName":"p","node":"k"}`}
	if p, ok := n.SlinkyPod(); !ok || p.Node != "k" {
		t.Errorf("got %+v %v", p, ok)
	}
	for _, c := range []string{"", "not json", `{"namespace":"slurm"}`} {
		if _, ok := (slurm.Node{Comment: c}).SlinkyPod(); ok {
			t.Errorf("comment %q should not parse as a Slinky pod", c)
		}
	}
}

func TestSlinkyMirrorsProblemClassNotKubernetesActions(t *testing.T) {
	// k8s-w1 cordoned for maintenance with pods still on it: the Kubernetes
	// recommendation is "drain" (finish evicting). The Slurm node, already
	// drained by the operator, must not be told to "drain".
	s := slinkyFixture(t, "slinky-cordoned-readonly", true)
	for i := range s.Kube.Nodes {
		if s.Kube.Nodes[i].Metadata.Name == "k8s-w1" {
			// no fault, just the cordon
			s.Kube.Nodes[i].Status.Conditions = kubeNode("x", map[string]string{"Ready": "True"}).Status.Conditions
		}
	}
	if kr := mustAssessKube(t, *s.Kube, "k8s-w1"); kr.Action != ActionDrain {
		t.Fatalf("precondition: k8s-w1 should be 'drain', got %s", kr.Action)
	}
	if r := mustAssess(t, s, "slinky-0"); r.Action != ActionWait {
		t.Errorf("slinky-0: got %s, want wait (nothing to do in Slurm)", r.Action)
	}
}

// drills/: slinky-pod-kill. Deleting a worker pod makes the operator mark the
// Slurm node DOWN ("slurm-operator: Pod is terminating") and requeue its job.
// The replacement pod registers within seconds, but Slurm keeps the node DOWN
// (Slinky runs ReturnToService=0) until someone resumes it.
func TestSlinkyReplacedPodNeedsResume(t *testing.T) {
	for _, scenario := range []string{"slinky-pod-replaced-early", "slinky-pod-replaced"} {
		r := mustAssess(t, slinkyFixture(t, scenario, true), "slinky-0")
		expect(t, r, ActionResume, SeverityInfo, CategoryPodRestarted)
		if !contains(r.Evidence, "slurmd restarted") || !contains(r.Commands, "scontrol update nodename=slinky-0 state=resume") {
			t.Errorf("%s: evidence %v, commands %v", scenario, r.Evidence, r.Commands)
		}
	}
}

func TestSlinkyPodStillTerminatingWaits(t *testing.T) {
	s := slinkyFixture(t, "slinky-pod-replaced-early", true)
	for i := range s.Nodes {
		if s.Nodes[i].Name == "slinky-0" { // the moment before the new slurmd registers
			s.Nodes[i].SlurmdStartTime.Number = s.Nodes[i].ReasonChangedAt.Number - 60
		}
	}
	expect(t, mustAssess(t, s, "slinky-0"), ActionWait, SeverityInfo, CategoryPodRestarted)
}

// drills/: k8s-pdb-drain. k8s-w1 cordoned for maintenance, its drain blocked by
// PDBs (one of them Slinky's own, protecting slinky-0's running job). The
// Kubernetes node's "investigate" is about the blocked drain, not a fault, so
// the busy Slurm node should just wait for its job.
func TestSlinkyBusyNodeOnMaintenanceDrainWaits(t *testing.T) {
	s := slinkyFixture(t, "slinky-drain-busy", true)
	if kr := mustAssessKube(t, *s.Kube, "k8s-w1"); kr.Action != ActionInvestigate || kr.Category != CategoryMaintenance {
		t.Fatalf("precondition: k8s-w1 should be investigate/maintenance (drain blocked), got %s/%s", kr.Action, kr.Category)
	}
	r := mustAssess(t, s, "slinky-0")
	expect(t, r, ActionWait, SeverityInfo, CategoryKubernetesManaged)
	if !strings.Contains(r.Summary, "1 job(s) running here finish first") {
		t.Errorf("summary: %q", r.Summary)
	}
}
