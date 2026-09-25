package server

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
)

// connect starts the server on an in-memory transport against a captured
// scenario and returns a connected MCP client session.
func connect(t *testing.T, scenario string, allowWrites bool) (*mcp.ClientSession, *slurm.FixtureRunner) {
	t.Helper()
	ctx := context.Background()
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatal(err)
	}
	fr := &slurm.FixtureRunner{Dir: filepath.Join("..", "slurm", "testdata", scenario)}
	at, err := fr.CapturedAt()
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		Client:      &slurm.Client{Runner: fr, Location: loc},
		AllowWrites: allowWrites,
		Now:         func() time.Time { return at },
		Version:     "test",
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
	return cs, fr
}

// call invokes a tool and decodes its structured output into out.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any, out any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	if res.IsError {
		t.Fatalf("%s returned a tool error: %+v", tool, res.Content)
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("%s: decoding output: %v", tool, err)
	}
	return res
}

func toolNames(t *testing.T, cs *mcp.ClientSession) []string {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

func TestWriteToolsOnlyWhenAllowed(t *testing.T) {
	ro, _ := connect(t, "healthy", false)
	if got, want := toolNames(t, ro), []string{"list_nodes", "node_detail", "triage_cluster", "triage_node"}; !slices.Equal(got, want) {
		t.Errorf("read-only server tools = %v, want %v", got, want)
	}
	rw, _ := connect(t, "healthy", true)
	if got := toolNames(t, rw); !slices.Contains(got, "drain_node") || !slices.Contains(got, "resume_node") {
		t.Errorf("-allow-writes server should add drain_node and resume_node, got %v", got)
	}
}

func TestTriageClusterPutsLostGPUFirst(t *testing.T) {
	cs, _ := connect(t, "gres-missing", false)
	var out TriageClusterOutput
	call(t, cs, "triage_cluster", nil, &out)
	if out.TotalNodes != 2 || out.HealthyNodes != 1 || len(out.Recommendations) != 1 {
		t.Fatalf("got %d total / %d healthy / %d recommendations, want 2/1/1", out.TotalNodes, out.HealthyNodes, len(out.Recommendations))
	}
	if r := out.Recommendations[0]; r.Node != "slurm-c1" || r.Action != "escalate_hardware" {
		t.Errorf("got %s %s, want slurm-c1 escalate_hardware", r.Node, r.Action)
	}
}

func TestNodeDetailShowsBlockingJob(t *testing.T) {
	cs, _ := connect(t, "draining", false)
	var out NodeDetailOutput
	call(t, cs, "node_detail", map[string]any{"node": "slurm-c1"}, &out)
	if len(out.RunningJobs) != 1 || out.RunningJobs[0].Name != "long-train" || out.RunningJobs[0].End != "no time limit" {
		t.Fatalf("running jobs = %+v, want long-train with no time limit", out.RunningJobs)
	}
	if !slices.Contains(out.Node.State, "DRAIN") {
		t.Errorf("state = %v, want DRAIN", out.Node.State)
	}
}

func TestResumeRefusesHardwareFault(t *testing.T) {
	cs, fr := connect(t, "gres-missing", true)
	var out WriteOutput
	call(t, cs, "resume_node", map[string]any{"node": "slurm-c1"}, &out)
	if out.Done || !strings.HasPrefix(out.Note, "refused") {
		t.Fatalf("resume of a hardware-faulted node: done=%v note=%q, want refused", out.Done, out.Note)
	}
	if w := fr.Writes(); len(w) != 0 {
		t.Fatalf("nothing should have been written, got %v", w)
	}

	call(t, cs, "resume_node", map[string]any{"node": "slurm-c1", "force": true}, &out)
	if !out.Done || !slices.Equal(fr.Writes(), []string{"scontrol update nodename=slurm-c1 state=resume"}) {
		t.Fatalf("forced resume: done=%v writes=%v", out.Done, fr.Writes())
	}
}

func TestDrainRecordsTriagePrefix(t *testing.T) {
	cs, fr := connect(t, "healthy", true)
	var out WriteOutput
	call(t, cs, "drain_node", map[string]any{"node": "slurm-c2", "reason": "2 NODE_FAIL jobs in 1h"}, &out)
	want := "scontrol update nodename=slurm-c2 state=drain reason=triage: 2 NODE_FAIL jobs in 1h"
	if !out.Done || !slices.Equal(fr.Writes(), []string{want}) {
		t.Fatalf("done=%v writes=%v, want %q", out.Done, fr.Writes(), want)
	}
}

func TestDrainNeedsReason(t *testing.T) {
	cs, fr := connect(t, "healthy", true)
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "drain_node", Arguments: map[string]any{"node": "slurm-c2", "reason": "  "}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || len(fr.Writes()) != 0 {
		t.Fatalf("an empty reason must be rejected without writing: isError=%v writes=%v", res.IsError, fr.Writes())
	}
}
