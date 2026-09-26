package triage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/kube"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
)

// kubeFixture loads a scenario captured from the lab cluster by
// scripts/capture-k8s-scenarios.sh.
func kubeFixture(t *testing.T, scenario string) KubeSnapshot {
	t.Helper()
	dir := filepath.Join("..", "kube", "testdata", scenario)
	src := &kube.FixtureSource{Dir: dir}
	ctx := context.Background()
	var s KubeSnapshot
	var err error
	if s.Now, err = (&slurm.FixtureRunner{Dir: dir}).CapturedAt(); err != nil {
		t.Fatal(err)
	}
	if s.Nodes, err = src.Nodes(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Pods, err = src.Pods(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Events, err = src.NodeEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if s.PDBs, err = src.PDBs(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustAssessKube(t *testing.T, s KubeSnapshot, node string) Recommendation {
	t.Helper()
	r, err := AssessKube(s, node, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Scheduler != SchedulerKubernetes {
		t.Errorf("scheduler = %q, want kubernetes", r.Scheduler)
	}
	return r
}

func expect(t *testing.T, r Recommendation, action Action, sev Severity, cat Category) {
	t.Helper()
	if r.Action != action || r.Severity != sev || r.Category != cat {
		t.Fatalf("%s: got %s/%s/%s, want %s/%s/%s\n  summary: %s\n  evidence: %v",
			r.Node, r.Action, r.Severity, r.Category, action, sev, cat, r.Summary, r.Evidence)
	}
}

// --- scenarios captured from the real cluster ------------------------------------

func TestKubeHealthyClusterNeedsNothing(t *testing.T) {
	for _, r := range AssessKubeAll(kubeFixture(t, "healthy"), Policy{}) {
		if r.Action != ActionNone {
			t.Errorf("%s: %s (%s)", r.Node, r.Action, r.Summary)
		}
	}
}

func TestKubeReadonlyFilesystemIsHardware(t *testing.T) {
	// ext4's "Remounting filesystem read-only" injected into k8s-w1's kmsg.
	s := kubeFixture(t, "readonly-fs")
	r := mustAssessKube(t, s, "k8s-w1")
	expect(t, r, ActionEscalateHardware, SeverityCritical, CategoryHardware)
	if !contains(r.Evidence, "ReadonlyFilesystem=True (FilesystemIsReadOnly)") || !contains(r.Commands, "kubectl cordon k8s-w1") {
		t.Errorf("want the NPD condition in evidence and a cordon: %v / %v", r.Evidence, r.Commands)
	}
	if all := AssessKubeAll(s, Policy{}); all[0].Node != "k8s-w1" {
		t.Errorf("the hardware fault should sort first, got %s", all[0].Node)
	}
}

func TestKubeKernelDeadlock(t *testing.T) {
	// "task docker:4242 blocked for more than 120 seconds." on k8s-w1.
	r := mustAssessKube(t, kubeFixture(t, "kernel-deadlock"), "k8s-w1")
	expect(t, r, ActionInvestigate, SeverityCritical, CategoryStuckProcess)
	if !contains(r.Evidence, "KernelDeadlock=True (DockerHung)") {
		t.Errorf("evidence: %v", r.Evidence)
	}
}

func TestKubeCordonBlockedByPDB(t *testing.T) {
	// k8s-w2 cordoned for maintenance while a 2-replica deployment pinned to it
	// has a minAvailable=2 PDB: a drain would hang.
	r := mustAssessKube(t, kubeFixture(t, "cordoned-pdb"), "k8s-w2")
	expect(t, r, ActionInvestigate, SeverityWarning, CategoryMaintenance)
	if !strings.Contains(r.Summary, "drain is blocked") || !contains(r.Evidence, "PDB triage-drill/checkout-api allows 0 disruptions") {
		t.Errorf("want the blocking PDB: %q %v", r.Summary, r.Evidence)
	}
	if !contains(r.Evidence, `cordon reason "maint: kernel update CHG-2044"`) {
		t.Errorf("want the cordon reason: %v", r.Evidence)
	}
}

func TestKubeKubeletUnreachable(t *testing.T) {
	// kubelet stopped on k8s-w2; captured after the unreachable taints landed,
	// before the 300 s eviction window ran out.
	r := mustAssessKube(t, kubeFixture(t, "notready"), "k8s-w2")
	expect(t, r, ActionInvestigate, SeverityCritical, CategoryUnreachable)
	if !strings.Contains(r.Summary, "will be evicted in") || !contains(r.Evidence, "unreachable:NoExecute taint added") {
		t.Errorf("want the eviction countdown: %q %v", r.Summary, r.Evidence)
	}
	// In this capture lastHeartbeatTime was 2m39s old while the node had been
	// Unknown for 11s: status updates are periodic, the lease is the liveness
	// signal. Don't present the stale timestamp as the last contact.
	if contains(r.Evidence, "heartbeat") || !contains(r.Evidence, "node lease stopped renewing") {
		t.Errorf("evidence should explain Unknown via the lease, not a stale heartbeat: %v", r.Evidence)
	}
}

func TestKubeRepeatedKernelEvents(t *testing.T) {
	// Three TaskHung kernel events on k8s-w2, which is otherwise Ready.
	r := mustAssessKube(t, kubeFixture(t, "task-hung-events"), "k8s-w2")
	expect(t, r, ActionInvestigate, SeverityWarning, CategoryKernelEvents)
	if !contains(r.Evidence, "TaskHung×3") {
		t.Errorf("evidence: %v", r.Evidence)
	}
}

// --- rules without a captured scenario ------------------------------------------

func kubeNode(name string, conds map[string]string) kube.Node {
	var n kube.Node
	n.Metadata.Name = name
	n.Status.NodeInfo.KubeletVersion = "v1.36.5"
	for typ, status := range conds {
		n.Status.Conditions = append(n.Status.Conditions, kube.Condition{Type: typ, Status: status, Reason: typ + "Reason", Message: typ + " message"})
	}
	return n
}

func pod(ns, name, node string, labels map[string]string, owner string) kube.Pod {
	var p kube.Pod
	p.Metadata.Namespace, p.Metadata.Name, p.Metadata.Labels = ns, name, labels
	if owner != "" {
		p.Metadata.OwnerReferences = []kube.OwnerReference{{Kind: owner, Name: "x"}}
	}
	p.Spec.NodeName = node
	p.Status.Phase = "Running"
	return p
}

var kubeNow = time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC)

func TestKubeCordonedEmptyForMaintenanceResumes(t *testing.T) {
	n := kubeNode("w", map[string]string{"Ready": "True"})
	n.Spec.Unschedulable = true
	n.Metadata.Annotations = map[string]string{kube.ReasonAnnotation: "maint: firmware CHG-9"}
	// Only a DaemonSet pod left: drain skips those.
	s := KubeSnapshot{Now: kubeNow, Nodes: []kube.Node{n}, Pods: []kube.Pod{pod("kube-system", "cilium-x", "w", nil, "DaemonSet")}}
	r := mustAssessKube(t, s, "w")
	expect(t, r, ActionResume, SeverityInfo, CategoryMaintenance)
	if len(r.Preconditions) == 0 || !contains(r.Commands, "kubectl uncordon w") {
		t.Errorf("want a precondition and uncordon: %v %v", r.Preconditions, r.Commands)
	}
}

func TestKubeCordonedWithPodsAndNoPDBDrains(t *testing.T) {
	n := kubeNode("w", map[string]string{"Ready": "True"})
	n.Spec.Unschedulable = true
	s := KubeSnapshot{Now: kubeNow, Nodes: []kube.Node{n}, Pods: []kube.Pod{pod("app", "api-1", "w", map[string]string{"app": "api"}, "ReplicaSet")}}
	expect(t, mustAssessKube(t, s, "w"), ActionDrain, SeverityInfo, CategoryOther)
}

func TestKubePDBWithExpressionsIsNotClaimedAsBlocking(t *testing.T) {
	n := kubeNode("w", map[string]string{"Ready": "True"})
	n.Spec.Unschedulable = true
	var pdb kube.PDB
	pdb.Metadata.Namespace, pdb.Metadata.Name = "app", "api"
	pdb.Spec.Selector = &kube.LabelSelector{MatchLabels: map[string]string{"app": "api"}, MatchExpressions: []json.RawMessage{json.RawMessage(`{}`)}}
	s := KubeSnapshot{Now: kubeNow, Nodes: []kube.Node{n}, PDBs: []kube.PDB{pdb},
		Pods: []kube.Pod{pod("app", "api-1", "w", map[string]string{"app": "api"}, "ReplicaSet")}}
	if r := mustAssessKube(t, s, "w"); r.Action != ActionDrain {
		t.Errorf("a selector triage can't fully evaluate must not block: got %s", r.Action)
	}
}

func TestKubeMemoryPressure(t *testing.T) {
	s := KubeSnapshot{Now: kubeNow, Nodes: []kube.Node{kubeNode("w", map[string]string{"Ready": "True", "MemoryPressure": "True"})}}
	expect(t, mustAssessKube(t, s, "w"), ActionInvestigate, SeverityWarning, CategoryResourcePressure)
}

func TestKubeKubeletNotReady(t *testing.T) {
	s := KubeSnapshot{Now: kubeNow, Nodes: []kube.Node{kubeNode("w", map[string]string{"Ready": "False"})}}
	r := mustAssessKube(t, s, "w")
	expect(t, r, ActionInvestigate, SeverityCritical, CategoryKubelet)
	if !strings.Contains(r.Summary, "Ready message") {
		t.Errorf("want the kubelet's message: %q", r.Summary)
	}
}
