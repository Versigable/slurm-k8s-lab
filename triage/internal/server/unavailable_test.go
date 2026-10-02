package server

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/kube"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/triage"
)

// failing wraps a Slurm source and fails the calls a drill broke: node and job
// state (slurmctld down) or only accounting (slurmdbd down).
type failing struct {
	SlurmSource
	scheduler, accounting error
	hang                  bool // never answer (until the context ends)
}

func (f failing) Nodes(ctx context.Context) ([]slurm.Node, error) {
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.scheduler != nil {
		return nil, f.scheduler
	}
	return f.SlurmSource.Nodes(ctx)
}

func (f failing) History(ctx context.Context, w time.Duration) ([]slurm.HistoricalJob, error) {
	if f.accounting != nil {
		return nil, f.accounting
	}
	return f.SlurmSource.History(ctx, w)
}

func (f failing) Events(ctx context.Context, w time.Duration) ([]slurm.Event, error) {
	if f.accounting != nil {
		return nil, f.accounting
	}
	return f.SlurmSource.Events(ctx, w)
}

func connectWith(t *testing.T, classic SlurmSource, timeout time.Duration) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	loc, _ := time.LoadLocation("America/Denver")
	sl := &slurm.FixtureRunner{Dir: filepath.Join("..", "slurm", "testdata", "slinky-healthy")}
	at, err := sl.CapturedAt()
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		Client:        classic,
		Slinky:        &slurm.Client{Runner: sl, Location: loc},
		Kube:          &kube.FixtureSource{Dir: filepath.Join("..", "kube", "testdata", "slinky-healthy")},
		SourceTimeout: timeout,
		Now:           func() time.Time { return at },
		Version:       "test",
	})
	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func healthyClassic() SlurmSource {
	loc, _ := time.LoadLocation("America/Denver")
	return &slurm.Client{Runner: &slurm.FixtureRunner{Dir: filepath.Join("..", "slurm", "testdata", "healthy")}, Location: loc}
}

// drills/: slurmctld-outage. Triage used to fail outright for every cluster.
func TestControllerDownStillTriagesTheOtherClusters(t *testing.T) {
	down := errors.New("scontrol show node --json: exit status 1: slurm_load_node error: Unable to contact slurm controller (connect failure)")
	cs := connectWith(t, failing{SlurmSource: healthyClassic(), scheduler: down}, time.Second)
	var out TriageClusterOutput
	call(t, cs, "triage_cluster", nil, &out)
	if out.TotalNodes != 5 {
		t.Errorf("total_nodes = %d, want 5 (2 Slinky + 3 Kubernetes still triaged)", out.TotalNodes)
	}
	if len(out.Unavailable) != 1 || out.Unavailable[0].Cluster != "lab" || out.Unavailable[0].Component != "slurmctld" {
		t.Fatalf("unavailable = %+v", out.Unavailable)
	}
	if !strings.HasPrefix(out.Unavailable[0].Error, "slurm_load_node error: Unable to contact") {
		t.Errorf("the error should be the CLI's own last line: %q", out.Unavailable[0].Error)
	}
	r := out.Recommendations[0]
	if r.Node != "slurmctld" || r.Category != triage.CategoryControlPlane || r.Severity != triage.SeverityCritical {
		t.Errorf("the outage should top the list as a critical control_plane entry, got %+v", r)
	}
	// Asking about a node of the unreachable cluster explains why it can't be found.
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "triage_node", Arguments: map[string]any{"node": "slurm-c1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, "unavailable: lab (slurmctld") {
		t.Errorf("triage_node on an unreachable cluster's node: %+v", res.Content)
	}
}

// drills/: slurmdbd-outage. Triage used to fail outright when sacct did.
func TestAccountingDownStillTriagesFromLiveState(t *testing.T) {
	dbd := errors.New("sacct --json -a -S now-1440minutes: exit status 1: sacct: error: Problem talking to the database: Connection refused")
	cs := connectWith(t, failing{SlurmSource: healthyClassic(), accounting: dbd}, time.Second)
	var out TriageClusterOutput
	call(t, cs, "triage_cluster", nil, &out)
	if out.TotalNodes != 7 {
		t.Errorf("total_nodes = %d, want all 7: node state doesn't need accounting", out.TotalNodes)
	}
	if len(out.Unavailable) != 1 || out.Unavailable[0].Component != "slurmdbd" {
		t.Fatalf("unavailable = %+v", out.Unavailable)
	}
	if r := out.Recommendations[0]; r.Node != "slurmdbd" || r.Category != triage.CategoryAccounting || r.Severity != triage.SeverityWarning {
		t.Errorf("want a warning about accounting, got %+v", r)
	}
	var rec triage.Recommendation
	call(t, cs, "triage_node", map[string]any{"node": "slurm-c1"}, &rec)
	if rec.Action != triage.ActionNone || !slices.ContainsFunc(rec.Evidence, func(e string) bool { return strings.Contains(e, "accounting unavailable") }) {
		t.Errorf("a healthy node is still healthy, with a note about what wasn't checked: %+v", rec)
	}
}

// A source that never answers costs SourceTimeout, not the CLI's retry budget,
// and doesn't hold up the others.
func TestHungSourceIsBoundedByTheTimeout(t *testing.T) {
	cs := connectWith(t, failing{SlurmSource: healthyClassic(), hang: true}, 200*time.Millisecond)
	start := time.Now()
	var out TriageClusterOutput
	call(t, cs, "triage_cluster", nil, &out)
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("took %s with a 200ms source timeout", took)
	}
	if len(out.Unavailable) != 1 || out.Unavailable[0].Error != "no answer within 200ms" {
		t.Errorf("unavailable = %+v", out.Unavailable)
	}
}
