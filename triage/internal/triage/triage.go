// Package triage turns a snapshot of Slurm state into a recommendation for
// one node: what an on-call compute-production engineer should do next, and
// the evidence behind it.
//
// The rules are deterministic on purpose. An LLM client decides which node to
// look at and explains the result; it never makes the call itself, and the
// same snapshot always produces the same recommendation.
package triage

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
)

// Action is what the operator should do with the node.
type Action string

const (
	ActionNone             Action = "none"              // healthy, leave it alone
	ActionWait             Action = "wait"              // draining; let running jobs finish
	ActionDrain            Action = "drain"             // take it out of scheduling
	ActionResume           Action = "resume"            // return it to service (check preconditions first)
	ActionInvestigate      Action = "investigate"       // needs a human look before any state change
	ActionEscalateHardware Action = "escalate_hardware" // keep it out; hardware ticket / RMA path
)

// Severity orders recommendations for an on-call view.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

func (s Severity) rank() int {
	switch s {
	case SeverityCritical:
		return 2
	case SeverityWarning:
		return 1
	}
	return 0
}

// Category classifies a node's drain/down reason.
type Category string

const (
	CategoryNone         Category = "none"
	CategoryHardware     Category = "hardware"
	CategoryHealthCheck  Category = "health_check"
	CategoryMaintenance  Category = "maintenance"
	CategoryStuckProcess Category = "stuck_process"
	CategoryUnreachable  Category = "unreachable"
	CategoryTriage       Category = "triage" // drained by this tool
	CategoryOther        Category = "other"
)

// Recommendation is the answer for one node.
type Recommendation struct {
	Node      string   `json:"node" jsonschema:"node name"`
	Scheduler string   `json:"scheduler" jsonschema:"slurm or kubernetes"`
	Action    Action   `json:"action" jsonschema:"one of none, wait, drain, resume, investigate, escalate_hardware"`
	Severity  Severity `json:"severity" jsonschema:"info, warning or critical"`
	Category  Category `json:"category" jsonschema:"classification of the node's drain/down reason"`
	Summary   string   `json:"summary" jsonschema:"one-line explanation"`
	Evidence  []string `json:"evidence" jsonschema:"facts from Slurm that support the recommendation"`
	// Preconditions must be true before acting (the tool can't verify them).
	Preconditions []string `json:"preconditions,omitempty" jsonschema:"things a human must confirm before acting"`
	NextSteps     []string `json:"next_steps,omitempty" jsonschema:"what to do, in order"`
	// Commands are suggestions for the operator. Triage never runs them.
	Commands []string `json:"commands,omitempty" jsonschema:"suggested commands; triage never runs them"`
}

// Snapshot is everything triage needs, taken at one instant.
type Snapshot struct {
	Now     time.Time
	Nodes   []slurm.Node
	Jobs    []slurm.Job
	History []slurm.HistoricalJob // recent sacct records
	Events  []slurm.Event         // recent node drain/down events
}

// Policy holds the thresholds. Zero values fall back to DefaultPolicy.
type Policy struct {
	// NODE_FAIL jobs on a healthy node (within the history window) before recommending a drain.
	NodeFailDrainThreshold int
	// Drain/down events on one node (within the event window) that mark it as chronic.
	ChronicEventThreshold int
	// node-problem-detector kernel events on a Ready Kubernetes node before it's flagged.
	KubeEventThreshold int
}

// DefaultPolicy is deliberately conservative for a small cluster.
var DefaultPolicy = Policy{NodeFailDrainThreshold: 2, ChronicEventThreshold: 3, KubeEventThreshold: 3}

func (p Policy) withDefaults() Policy {
	if p.NodeFailDrainThreshold <= 0 {
		p.NodeFailDrainThreshold = DefaultPolicy.NodeFailDrainThreshold
	}
	if p.ChronicEventThreshold <= 0 {
		p.ChronicEventThreshold = DefaultPolicy.ChronicEventThreshold
	}
	if p.KubeEventThreshold <= 0 {
		p.KubeEventThreshold = DefaultPolicy.KubeEventThreshold
	}
	return p
}

var categoryPatterns = []struct {
	cat Category
	re  *regexp.Regexp
}{
	{CategoryTriage, regexp.MustCompile(`(?i)^triage:`)},
	{CategoryHardware, regexp.MustCompile(`(?i)gres/\S+ count|xid|ecc|nvlink|pcie|fell off the bus|gpu (missing|lost)`)},
	{CategoryStuckProcess, regexp.MustCompile(`(?i)kill task failed`)},
	{CategoryUnreachable, regexp.MustCompile(`(?i)not responding`)},
	{CategoryHealthCheck, regexp.MustCompile(`(?i)^nhc|health ?check`)},
	{CategoryMaintenance, regexp.MustCompile(`(?i)^maint|\bchg-\d+|kernel update|firmware|scheduled reboot`)},
}

var gresShortfall = regexp.MustCompile(`gres/(\S+) count reported lower than configured \((\d+) < (\d+)\)`)

// Classify maps a free-text Slurm reason to a Category.
func Classify(reason string) Category {
	if strings.TrimSpace(reason) == "" {
		return CategoryNone
	}
	for _, p := range categoryPatterns {
		if p.re.MatchString(reason) {
			return p.cat
		}
	}
	return CategoryOther
}

// Assess returns the recommendation for one node.
func Assess(s Snapshot, name string, p Policy) (Recommendation, error) {
	p = p.withDefaults()
	i := slices.IndexFunc(s.Nodes, func(n slurm.Node) bool { return n.Name == name })
	if i < 0 {
		return Recommendation{}, fmt.Errorf("node %q not found", name)
	}
	n := s.Nodes[i]
	a := assessor{s: s, p: p, n: n}
	return a.assess(), nil
}

// AssessAll assesses every node, most severe first.
func AssessAll(s Snapshot, p Policy) []Recommendation {
	recs := make([]Recommendation, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		r, _ := Assess(s, n.Name, p)
		recs = append(recs, r)
	}
	SortRecommendations(recs)
	return recs
}

type assessor struct {
	s Snapshot
	p Policy
	n slurm.Node
}

func (a assessor) assess() Recommendation {
	n := a.n
	r := Recommendation{Node: n.Name, Scheduler: SchedulerSlurm, Category: Classify(n.Reason)}
	r.Evidence = append(r.Evidence, a.stateLine())
	if n.Reason != "" {
		r.Evidence = append(r.Evidence, a.reasonLine())
	}

	chronic := a.events()
	if len(chronic) >= a.p.ChronicEventThreshold {
		r.Evidence = append(r.Evidence, fmt.Sprintf("%d drain/down events for this node in the event window (chronic threshold %d)", len(chronic), a.p.ChronicEventThreshold))
	}
	waiting := a.waitingJobs()
	for _, j := range waiting {
		line := fmt.Sprintf("job %d %q (%s) is pending and can't start until this node is back", j.ID, j.Name, j.User)
		if j.RestartCount > 0 {
			line += fmt.Sprintf("; requeued %d time(s), likely by this node's failure", j.RestartCount)
		}
		r.Evidence = append(r.Evidence, line)
	}
	nodeFails := a.nodeFailJobs()
	if len(nodeFails) > 0 {
		r.Evidence = append(r.Evidence, fmt.Sprintf("%d job(s) ended NODE_FAIL on this node recently: %s", len(nodeFails), jobList(nodeFails)))
	}

	switch {
	case n.Has("INVALID") || n.Has("INVALID_REG") || r.Category == CategoryHardware:
		a.hardware(&r)
	case n.Has("DOWN"):
		a.down(&r, len(chronic) >= a.p.ChronicEventThreshold)
	case n.Has("NOT_RESPONDING"):
		a.unresponsive(&r)
	case n.Has("DRAIN"):
		a.drain(&r, len(chronic) >= a.p.ChronicEventThreshold)
	case len(nodeFails) >= a.p.NodeFailDrainThreshold:
		r.Action, r.Severity = ActionDrain, SeverityWarning
		reason := fmt.Sprintf("triage: %d NODE_FAIL jobs recently", len(nodeFails))
		r.Summary = fmt.Sprintf("Looks healthy but %d jobs failed on it with NODE_FAIL; drain before it eats more jobs.", len(nodeFails))
		r.NextSteps = []string{"Drain so running jobs finish and nothing new lands.", "Check slurmd and kernel logs around the failure times."}
		r.Commands = []string{fmt.Sprintf("scontrol update nodename=%s state=drain reason=%q", n.Name, reason)}
	default:
		r.Action, r.Severity = ActionNone, SeverityInfo
		r.Summary = "Healthy."
		if len(nodeFails) > 0 {
			r.Summary = fmt.Sprintf("Healthy; %d recent NODE_FAIL job(s) is below the drain threshold of %d.", len(nodeFails), a.p.NodeFailDrainThreshold)
		}
	}
	if len(waiting) > 0 && r.Action != ActionNone {
		r.NextSteps = append(r.NextSteps, "Jobs pinned to this node will wait until it's back. Tell the owners, or clear the pin so they can run elsewhere.")
		for _, j := range waiting {
			if j.RequiredNodes != "" {
				r.Commands = append(r.Commands, fmt.Sprintf("scontrol update jobid=%d ReqNodeList=   # clear the -w pin; ask %s first", j.ID, j.User))
			}
		}
	}
	return r
}

func (a assessor) hardware(r *Recommendation) {
	n := a.n
	r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
	r.Category = CategoryHardware
	r.Summary = "Hardware fault: the node registered with less hardware than configured. Keep it out of service."
	// e.g. "gres/gpu count reported lower than configured (3 < 4)". After such a
	// registration the node's gres field shows the reported count, not the configured one.
	configured := slurm.GresCount(n.Gres)
	if m := gresShortfall.FindStringSubmatch(n.Reason); m != nil {
		r.Evidence = append(r.Evidence, fmt.Sprintf("slurmd registered %s %s of %s configured", m[2], m[1], m[3]))
		configured, _ = strconv.Atoi(m[3])
	}
	r.NextSteps = []string{
		"Keep the node drained; don't resume it until the hardware is fixed.",
		"Open a hardware ticket with the reason text and node serial; on real GPUs, pull `nvidia-smi -q` and dmesg Xid lines first.",
		"After repair, run a burn-in job in the burnin partition before returning it to batch.",
	}
	r.Commands = []string{
		fmt.Sprintf("scontrol show node %s", n.Name),
		fmt.Sprintf("ssh %s 'journalctl -u slurmd -n 50; dmesg | tail -50'", n.Name),
		fmt.Sprintf("sbatch -p burnin -w %s --gres=gpu:%d burnin.sh   # after repair", n.Name, max(configured, 1)),
	}
}

func (a assessor) down(r *Recommendation, chronic bool) {
	n := a.n
	if chronic {
		r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
		r.Summary = "Down again; this node keeps failing. Treat it as a hardware/platform problem, not a one-off."
		r.NextSteps = []string{"Keep it out of service.", "Pull the event history into a hardware ticket.", "Burn-in before return to service."}
	} else {
		r.Action, r.Severity = ActionInvestigate, SeverityCritical
		r.Summary = "slurmd is unreachable. Find out whether the host, the network or just slurmd is gone."
		r.NextSteps = []string{
			"Check the host: power, console, network reachability.",
			"If the host is up, check slurmd (status, journal) and restart it.",
			"With ReturnToService=1 the node rejoins by itself once slurmd registers; otherwise resume it explicitly.",
		}
	}
	if n.Has("NOT_RESPONDING") {
		r.Category = CategoryUnreachable
	}
	r.Commands = []string{
		fmt.Sprintf("ping -c3 %s", n.Name),
		fmt.Sprintf("ssh %s 'systemctl status slurmd; journalctl -u slurmd -n 50'", n.Name),
		fmt.Sprintf("scontrol update nodename=%s state=resume   # only if it doesn't rejoin on its own", n.Name),
	}
}

// unresponsive: slurmctld has lost slurmd but hasn't marked the node DOWN yet.
// Jobs keep running (slurmstepd outlives slurmd), but once SlurmdTimeout
// expires the node goes DOWN and its jobs are killed or requeued. Restarting
// slurmd now saves them, which makes this the most time-sensitive state.
// Triage can't tell "slurmd died" from "the host died" (both look the same to
// slurmctld), so the wording reports what Slurm believes and the first
// command settles which case it is.
func (a assessor) unresponsive(r *Recommendation) {
	n := a.n
	r.Action, r.Severity, r.Category = ActionInvestigate, SeverityCritical, CategoryUnreachable
	running := a.runningJobs()
	for _, j := range running {
		r.Evidence = append(r.Evidence, fmt.Sprintf("job %d %q (%s) listed as RUNNING by slurmctld", j.ID, j.Name, j.User))
	}
	if len(running) > 0 {
		r.Summary = fmt.Sprintf("slurmd stopped responding; Slurm still lists %d job(s) as running. If the host is up, restart slurmd before SlurmdTimeout marks the node DOWN and kills them.", len(running))
	} else {
		r.Summary = "slurmd stopped responding. If the host is up, restart slurmd before SlurmdTimeout marks the node DOWN."
	}
	r.NextSteps = []string{
		"Check whether the host is reachable at all.",
		"If it is, restart slurmd; running jobs survive a slurmd restart.",
		"If the host is gone, the jobs are already lost. Let the timeout mark it DOWN and follow the down-node path.",
	}
	r.Commands = []string{
		fmt.Sprintf("ping -c3 %s", n.Name),
		fmt.Sprintf("ssh %s 'sudo systemctl restart slurmd && journalctl -u slurmd -n 20'", n.Name),
		"scontrol show config | grep -i SlurmdTimeout",
	}
}

func (a assessor) drain(r *Recommendation, chronic bool) {
	n := a.n
	running := a.runningJobs()
	if len(running) > 0 {
		r.Action, r.Severity = ActionWait, SeverityInfo
		var unbounded []slurm.Job
		var latest time.Time
		for _, j := range running {
			if j.Unbounded() {
				unbounded = append(unbounded, j)
				r.Evidence = append(r.Evidence, fmt.Sprintf("job %d %q (%s) running for %s with NO time limit", j.ID, j.Name, j.User, a.since(j.StartTime.Time())))
				continue
			}
			end := j.EndTime.Time()
			latest = maxTime(latest, end)
			r.Evidence = append(r.Evidence, fmt.Sprintf("job %d %q (%s) ends by %s", j.ID, j.Name, j.User, end.Format(time.RFC3339)))
		}
		if len(unbounded) > 0 {
			r.Severity = SeverityWarning
			r.Summary = fmt.Sprintf("Draining, but %d job(s) have no time limit, so the drain will never finish on its own.", len(unbounded))
			r.NextSteps = []string{"Ask the job owner to checkpoint and end it, or requeue it if it's requeue-safe."}
			for _, j := range unbounded {
				r.Commands = append(r.Commands, fmt.Sprintf("scontrol requeue %d   # only if the job is requeue-safe", j.ID))
			}
			return
		}
		r.Summary = fmt.Sprintf("Draining: %d job(s) still running; drained by %s at the latest.", len(running), latest.Format(time.RFC3339))
		r.NextSteps = []string{"Wait; nothing new will start here. Start the work once the node shows DRAINED."}
		return
	}

	if chronic {
		r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
		r.Summary = "Drained again; this node keeps getting pulled. Escalate instead of resuming it one more time."
		r.NextSteps = []string{"Keep it drained.", "Open a hardware/platform ticket with the event history.", "Burn-in before return to service."}
		return
	}

	switch r.Category {
	case CategoryMaintenance:
		r.Action, r.Severity = ActionResume, SeverityInfo
		r.Summary = fmt.Sprintf("Drained for maintenance %s ago with no jobs on it. Resume once the change is done.", a.since(n.ReasonChangedAt.Time()))
		r.Preconditions = []string{fmt.Sprintf("The maintenance in %q is complete and verified.", n.Reason)}
		r.Commands = []string{fmt.Sprintf("scontrol update nodename=%s state=resume", n.Name)}
	case CategoryHealthCheck:
		r.Action, r.Severity = ActionInvestigate, SeverityWarning
		r.Summary = "Drained by a health check. Fix what failed, re-run the check, and resume only if it passes."
		r.NextSteps = []string{"Fix the failing condition named in the reason.", "Re-run the health check on the node.", "Resume only if it passes."}
		r.Commands = []string{
			fmt.Sprintf("ssh %s 'sudo nhc'   # or whichever check drained it", n.Name),
			fmt.Sprintf("scontrol update nodename=%s state=resume   # after the check passes", n.Name),
		}
	case CategoryStuckProcess:
		r.Action, r.Severity = ActionInvestigate, SeverityWarning
		r.Summary = "slurmd couldn't kill a job's processes; usually hung I/O or a wedged device. A reboot is the normal fix."
		r.NextSteps = []string{"Look for processes stuck in D state.", "Reboot the node once it's idle and let it come back to service."}
		r.Commands = []string{
			fmt.Sprintf("ssh %s \"ps -eo pid,stat,wchan:32,cmd | awk '\\$2 ~ /D/'\"", n.Name),
			fmt.Sprintf("scontrol reboot ASAP nextstate=resume reason=\"stuck processes\" %s", n.Name),
		}
	default:
		r.Action, r.Severity = ActionInvestigate, SeverityWarning
		r.Summary = "Drained with a reason triage doesn't recognize. Ask whoever drained it before resuming."
		if n.ReasonSetBy != "" {
			r.NextSteps = []string{fmt.Sprintf("Check with %s, who set the reason.", n.ReasonSetBy)}
		}
	}
}

func (a assessor) stateLine() string {
	n := a.n
	line := fmt.Sprintf("state %s; CPUs %d/%d allocated", strings.Join(n.State, "+"), n.AllocCPUs, n.CPUs)
	if n.Gres != "" {
		line += fmt.Sprintf("; GRES %s (in use: %s)", n.Gres, n.GresUsed)
	}
	return line
}

func (a assessor) reasonLine() string {
	n := a.n
	line := fmt.Sprintf("reason %q", n.Reason)
	if t := n.ReasonChangedAt.Time(); !t.IsZero() {
		line += fmt.Sprintf(" set %s ago", a.since(t))
	}
	if n.ReasonSetBy != "" {
		line += " by " + n.ReasonSetBy
	}
	return line
}

func (a assessor) runningJobs() []slurm.Job {
	var out []slurm.Job
	for _, j := range a.s.Jobs {
		if slices.Contains(j.State, "RUNNING") && j.RunsOn(a.n.Name) {
			out = append(out, j)
		}
	}
	return out
}

func (a assessor) waitingJobs() []slurm.Job {
	var out []slurm.Job
	for _, j := range a.s.Jobs {
		if j.WaitingOn(a.n.Name) {
			out = append(out, j)
		}
	}
	return out
}

func (a assessor) nodeFailJobs() []slurm.HistoricalJob {
	var out []slurm.HistoricalJob
	for _, j := range a.s.History {
		if !slices.Contains(j.State.Current, "NODE_FAIL") {
			continue
		}
		if j.FailedNode == a.n.Name || (j.FailedNode == "" && j.OnNode(a.n.Name)) {
			out = append(out, j)
		}
	}
	return out
}

func (a assessor) events() []slurm.Event {
	var out []slurm.Event
	for _, e := range a.s.Events {
		if e.Node == a.n.Name {
			out = append(out, e)
		}
	}
	return out
}

func (a assessor) since(t time.Time) string {
	if t.IsZero() || a.s.Now.IsZero() {
		return "an unknown time"
	}
	d := a.s.Now.Sub(t)
	if d < 0 {
		d = 0
	}
	return d.Round(time.Second).String()
}

func jobList(jobs []slurm.HistoricalJob) string {
	parts := make([]string, 0, len(jobs))
	for _, j := range jobs {
		parts = append(parts, fmt.Sprintf("%d (%s)", j.ID, j.Name))
	}
	return strings.Join(parts, ", ")
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
