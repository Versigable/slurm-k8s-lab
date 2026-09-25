// Package server exposes Slurm node triage as MCP tools.
package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/triage"
)

// Config wires the server to a cluster.
type Config struct {
	Client        *slurm.Client
	Policy        triage.Policy
	HistoryWindow time.Duration // how far back to read sacct (NODE_FAIL evidence)
	EventWindow   time.Duration // how far back to read drain/down events (chronic nodes)
	// AllowWrites registers drain_node and resume_node. Off by default: without
	// it the server can't change cluster state at all.
	AllowWrites bool
	Now         func() time.Time
	Version     string
}

const instructions = `Slurm node triage for compute-production on-call.
Start with triage_cluster to see which nodes need attention, then triage_node or node_detail for one node.
Recommendations come from deterministic rules; quote the evidence and preconditions rather than paraphrasing them away.
Never tell the user a node was drained or resumed unless drain_node/resume_node returned done=true.`

// New builds the MCP server.
func New(cfg Config) *mcp.Server {
	if cfg.HistoryWindow == 0 {
		cfg.HistoryWindow = 24 * time.Hour
	}
	if cfg.EventWindow == 0 {
		cfg.EventWindow = 7 * 24 * time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	h := &handlers{cfg: cfg}

	s := mcp.NewServer(&mcp.Implementation{Name: "slurm-node-triage", Version: cfg.Version}, &mcp.ServerOptions{Instructions: instructions})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_nodes",
		Description: "List every Slurm node with its state flags, reason, CPU and GPU (GRES) usage.",
		Annotations: readOnly("List nodes"),
	}, h.listNodes)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "node_detail",
		Description: "Everything Slurm knows about one node: state, reason and who set it, running jobs, recent job outcomes, and drain/down events.",
		Annotations: readOnly("Node detail"),
	}, h.nodeDetail)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "triage_node",
		Description: "Recommend what to do with one node (none, wait, drain, resume, investigate, escalate_hardware), with the evidence, preconditions and suggested commands. Read-only: it never changes the cluster.",
		Annotations: readOnly("Triage node"),
	}, h.triageNode)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "triage_cluster",
		Description: "Triage every node and return the ones that need attention, most severe first.",
		Annotations: readOnly("Triage cluster"),
	}, h.triageCluster)

	if cfg.AllowWrites {
		mcp.AddTool(s, &mcp.Tool{
			Name:        "drain_node",
			Description: "Drain a node: running jobs finish, nothing new starts. The reason is recorded in Slurm prefixed with 'triage:'.",
			Annotations: write("Drain node", false),
		}, h.drainNode)

		mcp.AddTool(s, &mcp.Tool{
			Name:        "resume_node",
			Description: "Return a drained or down node to service. Refuses when triage says the node has a hardware fault, unless force is true.",
			Annotations: write("Resume node", false),
		}, h.resumeNode)
	}
	return s
}

func readOnly(title string) *mcp.ToolAnnotations {
	f := false
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, OpenWorldHint: &f}
}

func write(title string, destructive bool) *mcp.ToolAnnotations {
	f := false
	return &mcp.ToolAnnotations{Title: title, DestructiveHint: &destructive, IdempotentHint: true, OpenWorldHint: &f}
}

// --- tool inputs and outputs --------------------------------------------------

type NoInput struct{}

type NodeInput struct {
	Node string `json:"node" jsonschema:"Slurm node name, e.g. slurm-c1"`
}

type NodeSummary struct {
	Name        string   `json:"name"`
	State       []string `json:"state" jsonschema:"Slurm state flags, e.g. [IDLE DRAIN]"`
	Reason      string   `json:"reason,omitempty"`
	ReasonSetBy string   `json:"reason_set_by,omitempty"`
	ReasonSince string   `json:"reason_since,omitempty" jsonschema:"RFC3339 time the reason was set"`
	CPUs        string   `json:"cpus" jsonschema:"allocated/total"`
	Gres        string   `json:"gres,omitempty"`
	GresUsed    string   `json:"gres_used,omitempty"`
	Partitions  []string `json:"partitions,omitempty"`
}

type ListNodesOutput struct {
	Nodes []NodeSummary `json:"nodes"`
}

type JobSummary struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	User  string `json:"user"`
	State string `json:"state"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty" jsonschema:"end time, or 'no time limit'"`
}

type EventSummary struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Start  string `json:"start"`
	End    string `json:"end,omitempty" jsonschema:"empty while the event is still open"`
	User   string `json:"user"`
}

type NodeDetailOutput struct {
	Node        NodeSummary    `json:"node"`
	RunningJobs []JobSummary   `json:"running_jobs"`
	RecentJobs  []JobSummary   `json:"recent_jobs" jsonschema:"jobs that ran on this node within the history window"`
	Events      []EventSummary `json:"events" jsonschema:"drain/down events within the event window"`
}

type TriageClusterInput struct {
	IncludeHealthy bool `json:"include_healthy,omitempty" jsonschema:"also return nodes whose action is none"`
}

type TriageClusterOutput struct {
	Recommendations []triage.Recommendation `json:"recommendations"`
	HealthyNodes    int                     `json:"healthy_nodes"`
	TotalNodes      int                     `json:"total_nodes"`
}

type DrainInput struct {
	Node   string `json:"node" jsonschema:"Slurm node name"`
	Reason string `json:"reason" jsonschema:"why; recorded in Slurm as 'triage: <reason>'"`
}

type ResumeInput struct {
	Node  string `json:"node" jsonschema:"Slurm node name"`
	Force bool   `json:"force,omitempty" jsonschema:"resume even if triage recommends escalate_hardware"`
}

type WriteOutput struct {
	Done    bool   `json:"done"`
	Command string `json:"command" jsonschema:"the scontrol command that was run"`
	Note    string `json:"note,omitempty"`
}

// --- handlers --------------------------------------------------------------------

type handlers struct{ cfg Config }

func (h *handlers) snapshot(ctx context.Context) (triage.Snapshot, error) {
	c := h.cfg.Client
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return triage.Snapshot{}, err
	}
	jobs, err := c.Jobs(ctx)
	if err != nil {
		return triage.Snapshot{}, err
	}
	hist, err := c.History(ctx, h.cfg.HistoryWindow)
	if err != nil {
		return triage.Snapshot{}, err
	}
	events, err := c.Events(ctx, h.cfg.EventWindow)
	if err != nil {
		return triage.Snapshot{}, err
	}
	return triage.Snapshot{Now: h.cfg.Now(), Nodes: nodes, Jobs: jobs, History: hist, Events: events}, nil
}

func (h *handlers) listNodes(ctx context.Context, _ *mcp.CallToolRequest, _ NoInput) (*mcp.CallToolResult, ListNodesOutput, error) {
	nodes, err := h.cfg.Client.Nodes(ctx)
	if err != nil {
		return nil, ListNodesOutput{}, err
	}
	out := ListNodesOutput{Nodes: make([]NodeSummary, 0, len(nodes))}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, summarize(n))
	}
	return nil, out, nil
}

func (h *handlers) nodeDetail(ctx context.Context, _ *mcp.CallToolRequest, in NodeInput) (*mcp.CallToolResult, NodeDetailOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, NodeDetailOutput{}, err
	}
	var out NodeDetailOutput
	found := false
	for _, n := range s.Nodes {
		if n.Name == in.Node {
			out.Node, found = summarize(n), true
		}
	}
	if !found {
		return nil, NodeDetailOutput{}, fmt.Errorf("node %q not found", in.Node)
	}
	out.RunningJobs, out.RecentJobs, out.Events = []JobSummary{}, []JobSummary{}, []EventSummary{}
	for _, j := range s.Jobs {
		if j.RunsOn(in.Node) {
			end := formatTime(j.EndTime.Time())
			if j.Unbounded() {
				end = "no time limit"
			}
			out.RunningJobs = append(out.RunningJobs, JobSummary{ID: j.ID, Name: j.Name, User: j.User, State: strings.Join(j.State, "+"), Start: formatTime(j.StartTime.Time()), End: end})
		}
	}
	for _, j := range s.History {
		if j.OnNode(in.Node) || j.FailedNode == in.Node {
			out.RecentJobs = append(out.RecentJobs, JobSummary{ID: j.ID, Name: j.Name, User: j.User, State: strings.Join(j.State.Current, "+"), Start: formatUnix(j.Time.Start), End: formatUnix(j.Time.End)})
		}
	}
	for _, e := range s.Events {
		if e.Node == in.Node {
			out.Events = append(out.Events, EventSummary{State: e.State, Reason: e.Reason, Start: formatTime(e.Start), End: formatTime(e.End), User: e.User})
		}
	}
	return nil, out, nil
}

func (h *handlers) triageNode(ctx context.Context, _ *mcp.CallToolRequest, in NodeInput) (*mcp.CallToolResult, triage.Recommendation, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, triage.Recommendation{}, err
	}
	r, err := triage.Assess(s, in.Node, h.cfg.Policy)
	return nil, r, err
}

func (h *handlers) triageCluster(ctx context.Context, _ *mcp.CallToolRequest, in TriageClusterInput) (*mcp.CallToolResult, TriageClusterOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, TriageClusterOutput{}, err
	}
	all := triage.AssessAll(s, h.cfg.Policy)
	out := TriageClusterOutput{Recommendations: []triage.Recommendation{}, TotalNodes: len(all)}
	for _, r := range all {
		if r.Action == triage.ActionNone {
			out.HealthyNodes++
			if !in.IncludeHealthy {
				continue
			}
		}
		out.Recommendations = append(out.Recommendations, r)
	}
	return nil, out, nil
}

func (h *handlers) drainNode(ctx context.Context, _ *mcp.CallToolRequest, in DrainInput) (*mcp.CallToolResult, WriteOutput, error) {
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		return nil, WriteOutput{}, fmt.Errorf("a reason is required to drain a node")
	}
	if !strings.HasPrefix(strings.ToLower(reason), "triage:") {
		reason = "triage: " + reason
	}
	cmd := fmt.Sprintf("scontrol update nodename=%s state=drain reason=%q", in.Node, reason)
	if err := h.cfg.Client.Drain(ctx, in.Node, reason); err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	return nil, WriteOutput{Done: true, Command: cmd}, nil
}

func (h *handlers) resumeNode(ctx context.Context, _ *mcp.CallToolRequest, in ResumeInput) (*mcp.CallToolResult, WriteOutput, error) {
	cmd := fmt.Sprintf("scontrol update nodename=%s state=resume", in.Node)
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	r, err := triage.Assess(s, in.Node, h.cfg.Policy)
	if err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	if r.Action == triage.ActionEscalateHardware && !in.Force {
		return nil, WriteOutput{Command: cmd, Note: "refused: " + r.Summary + " Pass force=true only after the hardware is repaired and burn-in passed."}, nil
	}
	if err := h.cfg.Client.Resume(ctx, in.Node); err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	out := WriteOutput{Done: true, Command: cmd}
	if len(r.Preconditions) > 0 {
		out.Note = "resumed. Triage can't verify these preconditions; the caller is responsible for them: " + strings.Join(r.Preconditions, " ")
	}
	return nil, out, nil
}

func summarize(n slurm.Node) NodeSummary {
	return NodeSummary{
		Name:        n.Name,
		State:       n.State,
		Reason:      n.Reason,
		ReasonSetBy: n.ReasonSetBy,
		ReasonSince: formatTime(n.ReasonChangedAt.Time()),
		CPUs:        fmt.Sprintf("%d/%d", n.AllocCPUs, n.CPUs),
		Gres:        n.Gres,
		GresUsed:    n.GresUsed,
		Partitions:  n.Partitions,
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func formatUnix(sec int64) string {
	if sec == 0 {
		return ""
	}
	return formatTime(time.Unix(sec, 0))
}
