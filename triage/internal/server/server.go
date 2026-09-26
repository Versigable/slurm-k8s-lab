// Package server exposes Slurm and Kubernetes node triage as MCP tools.
package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/kube"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/triage"
)

// Config wires the server to the clusters it triages. Either scheduler may be
// absent (nil); at least one must be set.
type Config struct {
	Client        *slurm.Client // Slurm
	Kube          kube.Source   // Kubernetes
	Policy        triage.Policy
	HistoryWindow time.Duration // how far back to read sacct (NODE_FAIL evidence)
	EventWindow   time.Duration // how far back to read Slurm drain/down events (chronic nodes)
	// AllowWrites registers drain_node and resume_node. Off by default: without
	// it the server can't change cluster state at all.
	AllowWrites bool
	Now         func() time.Time
	Version     string
}

const instructions = `Node triage for compute-production on-call, across Slurm and Kubernetes.
Start with triage_cluster to see which nodes need attention (every recommendation says which scheduler it is about), then triage_node or node_detail for one node.
Recommendations come from deterministic rules; quote the evidence and preconditions rather than paraphrasing them away.
On Slurm, drain means no new jobs while running ones finish; on Kubernetes the equivalent is cordon (evicting pods is left to the operator).
Never tell the user a node was drained, cordoned or resumed unless drain_node/resume_node returned done=true.`

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

	s := mcp.NewServer(&mcp.Implementation{Name: "node-triage", Version: cfg.Version}, &mcp.ServerOptions{Instructions: instructions})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_nodes",
		Description: "List every Slurm and Kubernetes node with its state, reason, and (Slurm) CPU/GPU usage.",
		Annotations: readOnly("List nodes"),
	}, h.listNodes)

	mcp.AddTool(s, &mcp.Tool{
		Name: "node_detail",
		Description: "Everything known about one node. Slurm: state, reason and who set it, running jobs, recent job outcomes, drain/down events. " +
			"Kubernetes: conditions (incl. node-problem-detector), taints, cordon reason, workload pods, PDBs covering them, kernel events.",
		Annotations: readOnly("Node detail"),
	}, h.nodeDetail)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "triage_node",
		Description: "Recommend what to do with one node (none, wait, drain, resume, investigate, escalate_hardware), with the evidence, preconditions and suggested commands. Works for Slurm and Kubernetes nodes. Read-only.",
		Annotations: readOnly("Triage node"),
	}, h.triageNode)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "triage_cluster",
		Description: "Triage every Slurm and Kubernetes node and return the ones that need attention, most severe first.",
		Annotations: readOnly("Triage cluster"),
	}, h.triageCluster)

	if cfg.AllowWrites {
		mcp.AddTool(s, &mcp.Tool{
			Name: "drain_node",
			Description: "Take a node out of scheduling. Slurm: drain (running jobs finish, nothing new starts). " +
				"Kubernetes: cordon (running pods stay; nothing new is scheduled). The reason is recorded prefixed with 'triage:'.",
			Annotations: write("Drain node", false),
		}, h.drainNode)

		mcp.AddTool(s, &mcp.Tool{
			Name:        "resume_node",
			Description: "Return a node to service (Slurm resume, Kubernetes uncordon). Refuses when triage says the node has a hardware fault, unless force is true.",
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
	Node string `json:"node" jsonschema:"node name, e.g. slurm-c1 or k8s-w1"`
}

type NodeSummary struct {
	Name        string   `json:"name"`
	Scheduler   string   `json:"scheduler" jsonschema:"slurm or kubernetes"`
	State       []string `json:"state" jsonschema:"Slurm state flags, or Kubernetes Ready status plus cordoned and any True problem conditions"`
	Reason      string   `json:"reason,omitempty"`
	ReasonSetBy string   `json:"reason_set_by,omitempty"`
	ReasonSince string   `json:"reason_since,omitempty" jsonschema:"RFC3339 time the reason was set (Slurm)"`
	CPUs        string   `json:"cpus,omitempty" jsonschema:"allocated/total (Slurm)"`
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
	Node NodeSummary `json:"node"`
	// Slurm
	RunningJobs []JobSummary   `json:"running_jobs,omitempty"`
	RecentJobs  []JobSummary   `json:"recent_jobs,omitempty" jsonschema:"jobs that ran on this node within the history window"`
	Events      []EventSummary `json:"events,omitempty" jsonschema:"Slurm drain/down events within the event window"`
	// Kubernetes
	Conditions   []string `json:"conditions,omitempty" jsonschema:"every node condition: Type=Status (Reason) since"`
	Taints       []string `json:"taints,omitempty"`
	WorkloadPods []string `json:"workload_pods,omitempty" jsonschema:"pods a drain would evict (no DaemonSet or static pods)"`
	PDBs         []string `json:"pdbs,omitempty" jsonschema:"PodDisruptionBudgets covering those pods, with disruptions allowed"`
	KubeEvents   []string `json:"kube_events,omitempty" jsonschema:"recent events on the node object (node-problem-detector, kubelet)"`
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
	Node   string `json:"node" jsonschema:"node name (Slurm or Kubernetes)"`
	Reason string `json:"reason" jsonschema:"why; recorded as 'triage: <reason>'"`
}

type ResumeInput struct {
	Node  string `json:"node" jsonschema:"node name (Slurm or Kubernetes)"`
	Force bool   `json:"force,omitempty" jsonschema:"resume even if triage recommends escalate_hardware"`
}

type WriteOutput struct {
	Done    bool   `json:"done"`
	Command string `json:"command" jsonschema:"what was run (or would have been)"`
	Note    string `json:"note,omitempty"`
}

// --- snapshots -------------------------------------------------------------------------

type handlers struct{ cfg Config }

type snapshot struct {
	slurm *triage.Snapshot     // nil when Slurm isn't configured
	kube  *triage.KubeSnapshot // nil when Kubernetes isn't configured
}

func (h *handlers) snapshot(ctx context.Context) (snapshot, error) {
	var out snapshot
	now := h.cfg.Now()
	if c := h.cfg.Client; c != nil {
		s := triage.Snapshot{Now: now}
		var err error
		if s.Nodes, err = c.Nodes(ctx); err != nil {
			return out, fmt.Errorf("slurm: %w", err)
		}
		if s.Jobs, err = c.Jobs(ctx); err != nil {
			return out, fmt.Errorf("slurm: %w", err)
		}
		if s.History, err = c.History(ctx, h.cfg.HistoryWindow); err != nil {
			return out, fmt.Errorf("slurm: %w", err)
		}
		if s.Events, err = c.Events(ctx, h.cfg.EventWindow); err != nil {
			return out, fmt.Errorf("slurm: %w", err)
		}
		out.slurm = &s
	}
	if k := h.cfg.Kube; k != nil {
		s := triage.KubeSnapshot{Now: now}
		var err error
		if s.Nodes, err = k.Nodes(ctx); err != nil {
			return out, fmt.Errorf("kubernetes: %w", err)
		}
		if s.Pods, err = k.Pods(ctx); err != nil {
			return out, fmt.Errorf("kubernetes: %w", err)
		}
		if s.Events, err = k.NodeEvents(ctx); err != nil {
			return out, fmt.Errorf("kubernetes: %w", err)
		}
		if s.PDBs, err = k.PDBs(ctx); err != nil {
			return out, fmt.Errorf("kubernetes: %w", err)
		}
		out.kube = &s
	}
	return out, nil
}

// schedulerOf says which scheduler owns a node name.
func (s snapshot) schedulerOf(node string) (string, error) {
	if s.slurm != nil && slices.ContainsFunc(s.slurm.Nodes, func(n slurm.Node) bool { return n.Name == node }) {
		return triage.SchedulerSlurm, nil
	}
	if s.kube != nil && slices.ContainsFunc(s.kube.Nodes, func(n kube.Node) bool { return n.Metadata.Name == node }) {
		return triage.SchedulerKubernetes, nil
	}
	return "", fmt.Errorf("node %q not found in any configured scheduler", node)
}

func (h *handlers) assess(s snapshot, node string) (triage.Recommendation, error) {
	sched, err := s.schedulerOf(node)
	if err != nil {
		return triage.Recommendation{}, err
	}
	if sched == triage.SchedulerSlurm {
		return triage.Assess(*s.slurm, node, h.cfg.Policy)
	}
	return triage.AssessKube(*s.kube, node, h.cfg.Policy)
}

// --- handlers --------------------------------------------------------------------------

func (h *handlers) listNodes(ctx context.Context, _ *mcp.CallToolRequest, _ NoInput) (*mcp.CallToolResult, ListNodesOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, ListNodesOutput{}, err
	}
	out := ListNodesOutput{Nodes: []NodeSummary{}}
	if s.slurm != nil {
		for _, n := range s.slurm.Nodes {
			out.Nodes = append(out.Nodes, summarizeSlurm(n))
		}
	}
	if s.kube != nil {
		for _, n := range s.kube.Nodes {
			out.Nodes = append(out.Nodes, summarizeKube(n))
		}
	}
	return nil, out, nil
}

func (h *handlers) nodeDetail(ctx context.Context, _ *mcp.CallToolRequest, in NodeInput) (*mcp.CallToolResult, NodeDetailOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, NodeDetailOutput{}, err
	}
	sched, err := s.schedulerOf(in.Node)
	if err != nil {
		return nil, NodeDetailOutput{}, err
	}
	if sched == triage.SchedulerKubernetes {
		return nil, kubeDetail(*s.kube, in.Node), nil
	}
	return nil, slurmDetail(*s.slurm, in.Node), nil
}

func slurmDetail(s triage.Snapshot, node string) NodeDetailOutput {
	var out NodeDetailOutput
	for _, n := range s.Nodes {
		if n.Name == node {
			out.Node = summarizeSlurm(n)
		}
	}
	out.RunningJobs, out.RecentJobs, out.Events = []JobSummary{}, []JobSummary{}, []EventSummary{}
	for _, j := range s.Jobs {
		if j.RunsOn(node) {
			end := formatTime(j.EndTime.Time())
			if j.Unbounded() {
				end = "no time limit"
			}
			out.RunningJobs = append(out.RunningJobs, JobSummary{ID: j.ID, Name: j.Name, User: j.User, State: strings.Join(j.State, "+"), Start: formatTime(j.StartTime.Time()), End: end})
		}
	}
	for _, j := range s.History {
		if j.OnNode(node) || j.FailedNode == node {
			out.RecentJobs = append(out.RecentJobs, JobSummary{ID: j.ID, Name: j.Name, User: j.User, State: strings.Join(j.State.Current, "+"), Start: formatUnix(j.Time.Start), End: formatUnix(j.Time.End)})
		}
	}
	for _, e := range s.Events {
		if e.Node == node {
			out.Events = append(out.Events, EventSummary{State: e.State, Reason: e.Reason, Start: formatTime(e.Start), End: formatTime(e.End), User: e.User})
		}
	}
	return out
}

func kubeDetail(s triage.KubeSnapshot, node string) NodeDetailOutput {
	var out NodeDetailOutput
	var kn kube.Node
	for _, n := range s.Nodes {
		if n.Metadata.Name == node {
			kn = n
			out.Node = summarizeKube(n)
		}
	}
	for _, c := range kn.Status.Conditions {
		line := fmt.Sprintf("%s=%s", c.Type, c.Status)
		if c.Reason != "" {
			line += " (" + c.Reason + ")"
		}
		out.Conditions = append(out.Conditions, line+" since "+formatTime(c.LastTransitionTime))
	}
	for _, t := range kn.Spec.Taints {
		out.Taints = append(out.Taints, t.Key+":"+t.Effect)
	}
	var workload []kube.Pod
	for _, p := range s.Pods {
		if p.Spec.NodeName == node && p.Workload() {
			workload = append(workload, p)
			out.WorkloadPods = append(out.WorkloadPods, p.Ref())
		}
	}
	slices.Sort(out.WorkloadPods)
	for _, pdb := range s.PDBs {
		if slices.ContainsFunc(workload, pdb.Covers) {
			out.PDBs = append(out.PDBs, fmt.Sprintf("%s (disruptions allowed: %d)", pdb.Ref(), pdb.Status.DisruptionsAllowed))
		}
	}
	for _, e := range s.Events {
		if e.InvolvedObject.Name == node {
			out.KubeEvents = append(out.KubeEvents, fmt.Sprintf("%s %s %s x%d from %s: %s",
				formatTime(e.When()), e.Type, e.Reason, e.Occurrences(), e.Source.Component, e.Message))
		}
	}
	return out
}

func (h *handlers) triageNode(ctx context.Context, _ *mcp.CallToolRequest, in NodeInput) (*mcp.CallToolResult, triage.Recommendation, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, triage.Recommendation{}, err
	}
	r, err := h.assess(s, in.Node)
	return nil, r, err
}

func (h *handlers) triageCluster(ctx context.Context, _ *mcp.CallToolRequest, in TriageClusterInput) (*mcp.CallToolResult, TriageClusterOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, TriageClusterOutput{}, err
	}
	var all []triage.Recommendation
	if s.slurm != nil {
		all = append(all, triage.AssessAll(*s.slurm, h.cfg.Policy)...)
	}
	if s.kube != nil {
		all = append(all, triage.AssessKubeAll(*s.kube, h.cfg.Policy)...)
	}
	triage.SortRecommendations(all)
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
		return nil, WriteOutput{}, errors.New("a reason is required to drain a node")
	}
	if !strings.HasPrefix(strings.ToLower(reason), "triage:") {
		reason = "triage: " + reason
	}
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, WriteOutput{}, err
	}
	sched, err := s.schedulerOf(in.Node)
	if err != nil {
		return nil, WriteOutput{}, err
	}
	if sched == triage.SchedulerKubernetes {
		cmd := fmt.Sprintf("kubectl cordon %s && kubectl annotate node %s %s=%q", in.Node, in.Node, kube.ReasonAnnotation, reason)
		if err := h.cfg.Kube.SetUnschedulable(ctx, in.Node, true, reason); err != nil {
			return nil, WriteOutput{Command: cmd}, err
		}
		return nil, WriteOutput{Done: true, Command: cmd, Note: "cordoned: nothing new is scheduled; running pods stay until drained"}, nil
	}
	cmd := fmt.Sprintf("scontrol update nodename=%s state=drain reason=%q", in.Node, reason)
	if err := h.cfg.Client.Drain(ctx, in.Node, reason); err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	return nil, WriteOutput{Done: true, Command: cmd}, nil
}

func (h *handlers) resumeNode(ctx context.Context, _ *mcp.CallToolRequest, in ResumeInput) (*mcp.CallToolResult, WriteOutput, error) {
	s, err := h.snapshot(ctx)
	if err != nil {
		return nil, WriteOutput{}, err
	}
	sched, err := s.schedulerOf(in.Node)
	if err != nil {
		return nil, WriteOutput{}, err
	}
	cmd := fmt.Sprintf("scontrol update nodename=%s state=resume", in.Node)
	if sched == triage.SchedulerKubernetes {
		cmd = fmt.Sprintf("kubectl uncordon %s", in.Node)
	}
	r, err := h.assess(s, in.Node)
	if err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	if r.Action == triage.ActionEscalateHardware && !in.Force {
		return nil, WriteOutput{Command: cmd, Note: "refused: " + r.Summary + " Pass force=true only after the hardware is repaired and burn-in passed."}, nil
	}
	if sched == triage.SchedulerKubernetes {
		err = h.cfg.Kube.SetUnschedulable(ctx, in.Node, false, "")
	} else {
		err = h.cfg.Client.Resume(ctx, in.Node)
	}
	if err != nil {
		return nil, WriteOutput{Command: cmd}, err
	}
	out := WriteOutput{Done: true, Command: cmd}
	if len(r.Preconditions) > 0 {
		out.Note = "resumed. Triage can't verify these preconditions; the caller is responsible for them: " + strings.Join(r.Preconditions, " ")
	}
	return nil, out, nil
}

func summarizeSlurm(n slurm.Node) NodeSummary {
	return NodeSummary{
		Name:        n.Name,
		Scheduler:   triage.SchedulerSlurm,
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

func summarizeKube(n kube.Node) NodeSummary {
	ready, _ := n.Condition("Ready")
	state := []string{"Ready=" + ready.Status}
	if n.Spec.Unschedulable {
		state = append(state, "cordoned")
	}
	for _, c := range n.Status.Conditions {
		if c.Type != "Ready" && c.Status == "True" {
			state = append(state, c.Type)
		}
	}
	return NodeSummary{
		Name:      n.Metadata.Name,
		Scheduler: triage.SchedulerKubernetes,
		State:     state,
		Reason:    n.Metadata.Annotations[kube.ReasonAnnotation],
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
