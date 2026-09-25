package triage

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
)

// fixtureSnapshot loads a scenario captured from the lab cluster by
// scripts/capture-scenarios.sh.
func fixtureSnapshot(t *testing.T, scenario string) Snapshot {
	t.Helper()
	loc, err := time.LoadLocation("America/Denver") // the lab's sacctmgr time zone
	if err != nil {
		t.Fatal(err)
	}
	fr := &slurm.FixtureRunner{Dir: filepath.Join("..", "slurm", "testdata", scenario)}
	c := &slurm.Client{Runner: fr, Location: loc}
	ctx := context.Background()
	s := Snapshot{}
	if s.Now, err = fr.CapturedAt(); err != nil {
		t.Fatal(err)
	}
	if s.Nodes, err = c.Nodes(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Jobs, err = c.Jobs(ctx); err != nil {
		t.Fatal(err)
	}
	if s.History, err = c.History(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if s.Events, err = c.Events(ctx, 7*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustAssess(t *testing.T, s Snapshot, node string) Recommendation {
	t.Helper()
	r, err := Assess(s, node, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func contains(items []string, substr string) bool {
	return slices.ContainsFunc(items, func(s string) bool { return strings.Contains(s, substr) })
}

// --- scenarios captured from the real cluster ------------------------------------

func TestHealthyClusterNeedsNothing(t *testing.T) {
	s := fixtureSnapshot(t, "healthy")
	for _, r := range AssessAll(s, Policy{}) {
		if r.Action != ActionNone {
			t.Errorf("%s: action %s, want none (%s)", r.Node, r.Action, r.Summary)
		}
	}
}

func TestDrainedByHealthCheck(t *testing.T) {
	s := fixtureSnapshot(t, "drained")
	r := mustAssess(t, s, "slurm-c2")
	if r.Action != ActionInvestigate || r.Category != CategoryHealthCheck || r.Severity != SeverityWarning {
		t.Fatalf("got %s/%s/%s, want investigate/health_check/warning", r.Action, r.Category, r.Severity)
	}
	if !contains(r.Evidence, "NHC: check_fs_free") {
		t.Errorf("evidence should quote the reason: %v", r.Evidence)
	}
	if other := mustAssess(t, s, "slurm-c1"); other.Action != ActionNone {
		t.Errorf("slurm-c1: %s, want none", other.Action)
	}
}

func TestDrainingBlockedByUnboundedJob(t *testing.T) {
	s := fixtureSnapshot(t, "draining")
	r := mustAssess(t, s, "slurm-c1")
	if r.Action != ActionWait || r.Severity != SeverityWarning {
		t.Fatalf("got %s/%s, want wait/warning", r.Action, r.Severity)
	}
	if !strings.Contains(r.Summary, "no time limit") {
		t.Errorf("summary should explain the drain can't finish: %q", r.Summary)
	}
	if !contains(r.Evidence, `"long-train"`) || !contains(r.Commands, "scontrol requeue") {
		t.Errorf("want the blocking job in evidence and a requeue suggestion: %v / %v", r.Evidence, r.Commands)
	}
}

func TestLostGPUIsHardware(t *testing.T) {
	s := fixtureSnapshot(t, "gres-missing")
	r := mustAssess(t, s, "slurm-c1")
	if r.Action != ActionEscalateHardware || r.Severity != SeverityCritical || r.Category != CategoryHardware {
		t.Fatalf("got %s/%s/%s, want escalate_hardware/critical/hardware", r.Action, r.Severity, r.Category)
	}
	if !contains(r.Evidence, "registered 3 gpu of 4 configured") {
		t.Errorf("evidence should give reported vs configured: %v", r.Evidence)
	}
	// Burn-in must request the configured count, not the degraded one.
	if !contains(r.Commands, "--gres=gpu:4") {
		t.Errorf("burn-in should use the configured 4 GPUs: %v", r.Commands)
	}
	if all := AssessAll(s, Policy{}); all[0].Node != "slurm-c1" {
		t.Errorf("most severe node should sort first, got %s", all[0].Node)
	}
}

func TestSlurmdUnresponsiveWithRunningJob(t *testing.T) {
	// Captured 6 minutes after slurmd was SIGKILLed on slurm-c2: the controller
	// shows MIXED+NOT_RESPONDING and the job is still RUNNING (slurmstepd survives).
	s := fixtureSnapshot(t, "slurmd-unresponsive")
	r := mustAssess(t, s, "slurm-c2")
	if r.Action != ActionInvestigate || r.Severity != SeverityCritical || r.Category != CategoryUnreachable {
		t.Fatalf("got %s/%s/%s, want investigate/critical/unreachable", r.Action, r.Severity, r.Category)
	}
	if !strings.Contains(r.Summary, "still lists 1 job(s) as running") || !contains(r.Evidence, `"etl-shard-7"`) {
		t.Errorf("should warn that the running job is at risk: %q %v", r.Summary, r.Evidence)
	}
	if !contains(r.Commands, "systemctl restart slurmd") {
		t.Errorf("should suggest restarting slurmd: %v", r.Commands)
	}
}

func TestHostDownRequeuesPinnedJob(t *testing.T) {
	// Captured after slurm-c2 was force-powered-off mid-job. With JobRequeue=1
	// the batch job is requeued (PENDING, restart_cnt 1), not recorded as
	// NODE_FAIL, and because it was submitted with -w slurm-c2 it now waits for
	// the dead node.
	s := fixtureSnapshot(t, "node-down")
	r := mustAssess(t, s, "slurm-c2")
	if r.Action != ActionInvestigate || r.Severity != SeverityCritical || r.Category != CategoryUnreachable {
		t.Fatalf("got %s/%s/%s, want investigate/critical/unreachable", r.Action, r.Severity, r.Category)
	}
	if !contains(r.Evidence, `job 7 "etl-shard-9"`) || !contains(r.Evidence, "requeued 1 time(s)") {
		t.Errorf("evidence should name the requeued job waiting on this node: %v", r.Evidence)
	}
	if !contains(r.Commands, "scontrol update jobid=7 ReqNodeList=") {
		t.Errorf("should offer to clear the job's node pin: %v", r.Commands)
	}
	if other := mustAssess(t, s, "slurm-c1"); other.Action != ActionNone {
		t.Errorf("slurm-c1 should be healthy, got %s", other.Action)
	}
}

// --- rules without a captured scenario yet ----------------------------------------

var now = time.Date(2026, 9, 24, 22, 0, 0, 0, time.UTC)

func node(name string, reason string, state ...string) slurm.Node {
	return slurm.Node{
		Name: name, State: state, Reason: reason, CPUs: 2,
		ReasonChangedAt: slurm.Number{Set: true, Number: now.Add(-3 * time.Hour).Unix()},
	}
}

func TestMaintenanceDrainResumesWithPrecondition(t *testing.T) {
	s := Snapshot{Now: now, Nodes: []slurm.Node{node("n1", "maint: firmware CHG-2001", "IDLE", "DRAIN")}}
	r := mustAssess(t, s, "n1")
	if r.Action != ActionResume || len(r.Preconditions) == 0 {
		t.Fatalf("got %s with preconditions %v, want resume with a precondition", r.Action, r.Preconditions)
	}
	if !strings.Contains(r.Summary, "3h0m0s") {
		t.Errorf("summary should say how long it's been drained: %q", r.Summary)
	}
}

func TestChronicNodeEscalatesInsteadOfResume(t *testing.T) {
	s := Snapshot{Now: now, Nodes: []slurm.Node{node("n1", "maint: reboot CHG-1", "IDLE", "DRAIN")}}
	for i := range 3 {
		s.Events = append(s.Events, slurm.Event{Node: "n1", State: "DRAIN", Start: now.Add(-time.Duration(i+1) * 24 * time.Hour)})
	}
	if r := mustAssess(t, s, "n1"); r.Action != ActionEscalateHardware {
		t.Fatalf("got %s, want escalate_hardware for a node drained 3 times", r.Action)
	}
}

func TestRepeatedNodeFailOnHealthyNodeDrains(t *testing.T) {
	fail := func(id int64) slurm.HistoricalJob {
		j := slurm.HistoricalJob{ID: id, Name: "train", Nodes: "n[1-2]", FailedNode: "n1"}
		j.State.Current = []string{"NODE_FAIL"}
		return j
	}
	s := Snapshot{Now: now, Nodes: []slurm.Node{node("n1", "", "IDLE"), node("n2", "", "IDLE")}}

	s.History = []slurm.HistoricalJob{fail(10)}
	if r := mustAssess(t, s, "n1"); r.Action != ActionNone || !strings.Contains(r.Summary, "below the drain threshold") {
		t.Fatalf("one NODE_FAIL: got %s %q, want none with a threshold note", r.Action, r.Summary)
	}

	s.History = append(s.History, fail(11))
	r := mustAssess(t, s, "n1")
	if r.Action != ActionDrain || !contains(r.Commands, `reason="triage: 2 NODE_FAIL jobs recently"`) {
		t.Fatalf("two NODE_FAIL: got %s %v, want drain with a triage-prefixed reason", r.Action, r.Commands)
	}
	// failed_node pins the blame: the other node in the allocation is fine.
	if r := mustAssess(t, s, "n2"); r.Action != ActionNone {
		t.Errorf("n2 was only a bystander, got %s", r.Action)
	}
}

func TestStuckProcessDrain(t *testing.T) {
	s := Snapshot{Now: now, Nodes: []slurm.Node{node("n1", "Kill task failed", "IDLE", "DRAIN")}}
	r := mustAssess(t, s, "n1")
	if r.Category != CategoryStuckProcess || !contains(r.Commands, "scontrol reboot ASAP") {
		t.Fatalf("got %s %v, want stuck_process with a reboot suggestion", r.Category, r.Commands)
	}
}

func TestUnknownNode(t *testing.T) {
	if _, err := Assess(Snapshot{}, "nope", Policy{}); err == nil {
		t.Fatal("want an error for an unknown node")
	}
}

func TestClassify(t *testing.T) {
	tests := map[string]Category{
		"": CategoryNone,
		"gres/gpu count reported lower than configured (3 < 4)": CategoryHardware,
		"Xid 79 on GPU 3":               CategoryHardware,
		"NHC: check_hw_mem failed":      CategoryHealthCheck,
		"maint: kernel update CHG-1042": CategoryMaintenance,
		"Kill task failed":              CategoryStuckProcess,
		"Not responding":                CategoryUnreachable,
		"triage: 2 NODE_FAIL jobs":      CategoryTriage,
		"bob said so":                   CategoryOther,
	}
	for reason, want := range tests {
		if got := Classify(reason); got != want {
			t.Errorf("Classify(%q) = %s, want %s", reason, got, want)
		}
	}
}
