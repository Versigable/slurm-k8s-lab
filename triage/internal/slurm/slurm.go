// Package slurm reads cluster state from the Slurm CLIs (JSON output, Slurm
// 24.11 / data_parser v0.0.42) and performs the few node updates the triage
// server is allowed to make.
package slurm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/hostlist"
)

// Runner executes a Slurm CLI and returns its stdout.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// ExecRunner runs commands on the local host (the Slurm controller or a login node).
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// Number is Slurm's optional-number wrapper: {"set": bool, "infinite": bool, "number": n}.
type Number struct {
	Set      bool  `json:"set"`
	Infinite bool  `json:"infinite"`
	Number   int64 `json:"number"`
}

// Time interprets n as a Unix timestamp. Zero if unset, infinite or 0.
func (n Number) Time() time.Time {
	if !n.Set || n.Infinite || n.Number == 0 {
		return time.Time{}
	}
	return time.Unix(n.Number, 0)
}

// Node is the subset of `scontrol show node --json` that triage uses.
type Node struct {
	Name            string   `json:"name"`
	State           []string `json:"state"`
	Reason          string   `json:"reason"`
	ReasonChangedAt Number   `json:"reason_changed_at"`
	ReasonSetBy     string   `json:"reason_set_by_user"`
	CPUs            int      `json:"cpus"`
	AllocCPUs       int      `json:"alloc_cpus"`
	RealMemory      int64    `json:"real_memory"`
	AllocMemory     int64    `json:"alloc_memory"`
	Gres            string   `json:"gres"`
	GresUsed        string   `json:"gres_used"`
	GresDrained     string   `json:"gres_drained"`
	Partitions      []string `json:"partitions"`
	BootTime        Number   `json:"boot_time"`
	SlurmdStartTime Number   `json:"slurmd_start_time"`
	LastBusy        Number   `json:"last_busy"`
}

// Has reports whether the node carries a state flag (IDLE, DRAIN, DOWN, NOT_RESPONDING, ...).
func (n Node) Has(flag string) bool { return slices.Contains(n.State, flag) }

// Job is a running or pending job from `squeue --json`.
type Job struct {
	ID        int64    `json:"job_id"`
	Name      string   `json:"name"`
	User      string   `json:"user_name"`
	Account   string   `json:"account"`
	Partition string   `json:"partition"`
	State     []string `json:"job_state"`
	Nodes     string   `json:"nodes"`
	StartTime Number   `json:"start_time"`
	EndTime   Number   `json:"end_time"`
	TimeLimit Number   `json:"time_limit"` // minutes
	// Requeue and pending-reason fields: a job requeued after a node failure
	// shows up here as PENDING with RestartCount > 0, not as NODE_FAIL in sacct.
	RestartCount     int    `json:"restart_cnt"`
	RequiredNodes    string `json:"required_nodes"`
	StateReason      string `json:"state_reason"`
	StateDescription string `json:"state_description"` // e.g. "ReqNodeNotAvail, UnavailableNodes:slurm-c2"
}

// Unbounded reports whether the job has no time limit, so it will never end on its own.
func (j Job) Unbounded() bool { return j.TimeLimit.Infinite || !j.TimeLimit.Set }

// HistoricalJob is a job record from `sacct --json`.
type HistoricalJob struct {
	ID         int64  `json:"job_id"`
	Name       string `json:"name"`
	User       string `json:"user"`
	Nodes      string `json:"nodes"`
	FailedNode string `json:"failed_node"`
	State      struct {
		Current []string `json:"current"`
		Reason  string   `json:"reason"`
	} `json:"state"`
	Time struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"time"`
}

// Event is a node state change from `sacctmgr show event` (drains, downs).
type Event struct {
	Node   string
	Start  time.Time
	End    time.Time // zero while the event is still open
	State  string
	Reason string
	User   string
}

// Client reads Slurm state through a Runner.
type Client struct {
	Runner Runner
	// Location for sacctmgr's zone-less timestamps. Defaults to time.Local.
	Location *time.Location
}

// Nodes returns every node known to slurmctld.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var resp struct {
		Nodes []Node `json:"nodes"`
	}
	if err := c.runJSON(ctx, &resp, "scontrol", "show", "node", "--json"); err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

// Jobs returns the jobs currently in the queue (pending and running).
func (c *Client) Jobs(ctx context.Context) ([]Job, error) {
	var resp struct {
		Jobs []Job `json:"jobs"`
	}
	if err := c.runJSON(ctx, &resp, "squeue", "--json"); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

// History returns jobs from all users that ran within window.
func (c *Client) History(ctx context.Context, window time.Duration) ([]HistoricalJob, error) {
	var resp struct {
		Jobs []HistoricalJob `json:"jobs"`
	}
	start := fmt.Sprintf("now-%dminutes", int(window.Minutes()))
	if err := c.runJSON(ctx, &resp, "sacct", "--json", "-a", "-S", start); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

// Events returns node events (drain/down) within window.
func (c *Client) Events(ctx context.Context, window time.Duration) ([]Event, error) {
	start := fmt.Sprintf("now-%dminutes", int(window.Minutes()))
	out, err := c.Runner.Run(ctx, "sacctmgr", "-n", "-P", "show", "event", "event=node",
		"start="+start, "format=NodeName,TimeStart,TimeEnd,State,Reason,User")
	if err != nil {
		return nil, err
	}
	return parseEvents(out, c.location())
}

// Drain takes a node out of scheduling. Running jobs finish; nothing new starts.
func (c *Client) Drain(ctx context.Context, node, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("a drain reason is required")
	}
	_, err := c.Runner.Run(ctx, "scontrol", "update", "nodename="+node, "state=drain", "reason="+reason)
	return err
}

// Resume returns a drained or down node to service.
func (c *Client) Resume(ctx context.Context, node string) error {
	_, err := c.Runner.Run(ctx, "scontrol", "update", "nodename="+node, "state=resume")
	return err
}

func (c *Client) runJSON(ctx context.Context, v any, name string, args ...string) error {
	out, err := c.Runner.Run(ctx, name, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(out, v); err != nil {
		return fmt.Errorf("%s: decoding JSON: %w", name, err)
	}
	return nil
}

func (c *Client) location() *time.Location {
	if c.Location != nil {
		return c.Location
	}
	return time.Local
}

func parseEvents(out []byte, loc *time.Location) ([]Event, error) {
	var events []Event
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		f := strings.Split(line, "|")
		if len(f) < 6 || f[0] == "" { // cluster-level events have no node name
			continue
		}
		start, err := time.ParseInLocation("2006-01-02T15:04:05", f[1], loc)
		if err != nil {
			return nil, fmt.Errorf("event %q: %w", line, err)
		}
		var end time.Time
		if f[2] != "Unknown" && f[2] != "" {
			if end, err = time.ParseInLocation("2006-01-02T15:04:05", f[2], loc); err != nil {
				return nil, fmt.Errorf("event %q: %w", line, err)
			}
		}
		// A node event can name several nodes as a hostlist.
		nodes, err := hostlist.Expand(f[0])
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			events = append(events, Event{Node: n, Start: start, End: end, State: f[3], Reason: f[4], User: f[5]})
		}
	}
	return events, sc.Err()
}

// OnNode reports whether a historical job ran on node.
func (j HistoricalJob) OnNode(node string) bool {
	hosts, err := hostlist.Expand(j.Nodes)
	if err != nil {
		return false
	}
	return slices.Contains(hosts, node)
}

// WaitingOn reports whether a pending job can't start until node is available:
// it requires the node (-w) or Slurm lists the node as the one it's waiting for.
func (j Job) WaitingOn(node string) bool {
	if !slices.Contains(j.State, "PENDING") {
		return false
	}
	if hosts, err := hostlist.Expand(j.RequiredNodes); err == nil && slices.Contains(hosts, node) {
		return true
	}
	if _, unavailable, ok := strings.Cut(j.StateDescription, "UnavailableNodes:"); ok {
		hosts, err := hostlist.Expand(strings.TrimSpace(unavailable))
		return err == nil && slices.Contains(hosts, node)
	}
	return false
}

// RunsOn reports whether a queued job is running on node.
func (j Job) RunsOn(node string) bool {
	hosts, err := hostlist.Expand(j.Nodes)
	if err != nil {
		return false
	}
	return slices.Contains(hosts, node)
}

// GresCount returns the total count in a gres string such as "gpu:fake:4" or
// "gpu:fake:2(IDX:0-1)". Multiple entries are summed. Zero if none.
func GresCount(gres string) int {
	total := 0
	for _, entry := range strings.Split(gres, ",") {
		entry, _, _ = strings.Cut(entry, "(")
		fields := strings.Split(entry, ":")
		if len(fields) < 2 {
			continue
		}
		if n, err := strconv.Atoi(fields[len(fields)-1]); err == nil {
			total += n
		}
	}
	return total
}
