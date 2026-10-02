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
	ActionBurnIn           Action = "burn_in"           // not in production yet: run the burn-in job
	ActionPromote          Action = "promote"           // burn-in passed: add it to production
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
	// Drained by the Slinky operator because its Kubernetes node is cordoned.
	// The fix lives on the Kubernetes side.
	CategoryKubernetesManaged Category = "kubernetes_managed"
	// A cluster's scheduler or the Kubernetes API couldn't be read (see ComponentUnavailable).
	CategoryControlPlane Category = "control_plane"
	// Accounting (slurmdbd) couldn't be read; history-based checks are off.
	CategoryAccounting Category = "accounting"
	// A Slinky node whose pod was deleted and replaced; Slurm holds it DOWN until resumed.
	CategoryPodRestarted Category = "pod_restarted"
	// The node rebooted without being asked; Slurm holds it DOWN until a human returns it.
	CategoryUnexpectedReboot Category = "unexpected_reboot"
	// A Slinky node whose Kubernetes node is in trouble, while Slurm still sees it as fine.
	CategoryKubernetesNode Category = "kubernetes_node"
	// A node that isn't in production yet: burn-in decides whether it gets there.
	CategoryQualification Category = "qualification"
)

// Recommendation is the answer for one node.
type Recommendation struct {
	Node      string   `json:"node" jsonschema:"node name"`
	Scheduler string   `json:"scheduler" jsonschema:"slurm or kubernetes"`
	Cluster   string   `json:"cluster,omitempty" jsonschema:"which cluster: e.g. lab (classic Slurm), slinky (Slurm on Kubernetes), kubernetes"`
	Action    Action   `json:"action" jsonschema:"one of none, wait, drain, resume, investigate, escalate_hardware, burn_in, promote"`
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
	Cluster string // cluster name, e.g. "lab" or "slinky"
	// Kube, when set, lets Slinky nodes be linked to the Kubernetes node they run on.
	Kube    *KubeSnapshot
	Now     time.Time
	Nodes   []slurm.Node
	Jobs    []slurm.Job
	History []slurm.HistoricalJob // recent sacct records
	Events  []slurm.Event         // recent node drain/down events
	// AccountingError is set when History and Events couldn't be read; triage
	// then works from live state and says what it didn't check.
	AccountingError string
}

// Policy holds the thresholds. Zero values fall back to DefaultPolicy.
type Policy struct {
	// NODE_FAIL jobs on a healthy node (within the history window) before recommending a drain.
	NodeFailDrainThreshold int
	// Separate drain/down incidents on one node (within the event window) that
	// mark it as chronic. Back-to-back events count once; maintenance doesn't count.
	ChronicEventThreshold int
	// node-problem-detector kernel events on a Ready Kubernetes node before it's flagged.
	KubeEventThreshold int
	// Partitions of the node lifecycle: a node in BurninPartition but not in
	// ProductionPartition is still being qualified.
	ProductionPartition string
	BurninPartition     string
}

// DefaultPolicy is deliberately conservative for a small cluster.
var DefaultPolicy = Policy{NodeFailDrainThreshold: 2, ChronicEventThreshold: 3, KubeEventThreshold: 3,
	ProductionPartition: "batch", BurninPartition: "burnin"}

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
	if p.ProductionPartition == "" {
		p.ProductionPartition = DefaultPolicy.ProductionPartition
	}
	if p.BurninPartition == "" {
		p.BurninPartition = DefaultPolicy.BurninPartition
	}
	return p
}

var categoryPatterns = []struct {
	cat Category
	re  *regexp.Regexp
}{
	{CategoryKubernetesManaged, regexp.MustCompile(`^slurm-operator:`)},
	{CategoryTriage, regexp.MustCompile(`(?i)^triage:`)},
	{CategoryHardware, regexp.MustCompile(`(?i)^hardware\b|gres/\S+ count|xid|ecc|nvlink|pcie|fell off the bus|gpu (missing|lost)`)},
	{CategoryStuckProcess, regexp.MustCompile(`(?i)kill task failed`)},
	{CategoryUnreachable, regexp.MustCompile(`(?i)not responding`)},
	{CategoryUnexpectedReboot, regexp.MustCompile(`(?i)unexpectedly rebooted`)},
	{CategoryHealthCheck, regexp.MustCompile(`(?i)^nhc|health ?check`)},
	{CategoryMaintenance, regexp.MustCompile(`(?i)^maint|\bchg-\d+|kernel update|firmware|scheduled reboot`)},
}

var gresShortfall = regexp.MustCompile(`gres/(\S+) count reported lower than configured \((\d+) < (\d+)\)`)

// Classify maps a free-text Slurm reason to a Category.
//
// A drain triage made ("triage: ...") keeps the category of what it's about:
// re-labelling a health-check drain as "triage: hardware: GPU missing" (so NHC
// can't auto-resume it after the repair) must not turn a hardware fault into
// an unknown reason. Found by drills/: nhc-gpu-lost.
func Classify(reason string) Category {
	if strings.TrimSpace(reason) == "" {
		return CategoryNone
	}
	if len(reason) >= 7 && strings.EqualFold(reason[:7], "triage:") {
		if c := Classify(strings.TrimSpace(reason[7:])); c != CategoryNone && c != CategoryOther {
			return c
		}
		return CategoryTriage
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
	r := Recommendation{Node: n.Name, Scheduler: SchedulerSlurm, Cluster: a.s.Cluster, Category: Classify(n.Reason)}
	r.Evidence = append(r.Evidence, a.stateLine())
	if n.Reason != "" {
		r.Evidence = append(r.Evidence, a.reasonLine())
	}

	var kubeTrouble *Recommendation
	if pod, ok := n.SlinkyPod(); ok {
		r.Evidence = append(r.Evidence, fmt.Sprintf("Slinky node: runs as pod %s/%s on Kubernetes node %s", pod.Namespace, pod.PodName, pod.Node))
		if r.Category == CategoryKubernetesManaged {
			a.kubernetesManaged(&r, pod)
			return r
		}
		if kr, ok := a.kubeNodeTrouble(pod.Node); ok {
			r.Evidence = append(r.Evidence, fmt.Sprintf("Kubernetes node %s: %s/%s: %s", pod.Node, kr.Action, kr.Severity, kr.Summary))
			kubeTrouble = &kr
		}
	}

	incidents := a.incidents()
	chronic := incidents >= a.p.ChronicEventThreshold
	if chronic {
		r.Evidence = append(r.Evidence, fmt.Sprintf("%d separate drain/down incidents for this node in the event window (chronic threshold %d)", incidents, a.p.ChronicEventThreshold))
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
	if a.s.AccountingError != "" {
		r.Evidence = append(r.Evidence, "accounting unavailable, so recent NODE_FAIL jobs and drain/down history weren't checked")
	}

	switch {
	case n.Has("INVALID") || n.Has("INVALID_REG") || r.Category == CategoryHardware:
		a.hardware(&r)
	case n.Has("REBOOT_REQUESTED") || n.Has("REBOOT_ISSUED"):
		a.reboot(&r)
	case n.Has("DOWN"):
		a.down(&r, chronic)
	case n.Has("NOT_RESPONDING"):
		a.unresponsive(&r)
	case n.Has("DRAIN"):
		a.drain(&r, chronic)
	case a.inQualification():
		a.qualify(&r)
	case kubeTrouble != nil:
		a.onTroubledKubeNode(&r, *kubeTrouble)
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

// kubernetesManaged: the Slinky operator drained this node because its
// Kubernetes node is cordoned. Resuming it in Slurm is pointless: while the
// Kubernetes node stays cordoned the operator re-drains it (within 2 s in the
// lab). So the recommendation follows the Kubernetes node's.
func (a assessor) kubernetesManaged(r *Recommendation, pod slurm.SlinkyPod) {
	n := a.n
	if strings.Contains(n.Reason, "Pod is terminating") {
		a.podReplaced(r, pod)
		return
	}
	r.Action, r.Severity = ActionInvestigate, SeverityWarning
	r.Summary = fmt.Sprintf("Drained by the Slinky operator because Kubernetes node %s is cordoned. Handle it on the Kubernetes side; resuming this node in Slurm gets reverted.", pod.Node)
	if a.s.Kube != nil {
		if kr, err := AssessKube(*a.s.Kube, pod.Node, a.p); err == nil {
			r.Evidence = append(r.Evidence, fmt.Sprintf("Kubernetes node %s: %s/%s: %s", pod.Node, kr.Action, kr.Severity, kr.Summary))
			// Mirror the problem class (hardware, investigate), not actions that
			// are performed on the Kubernetes node: "drain" or "resume" here would
			// read as instructions for this already-drained Slurm node.
			switch {
			case kr.Action == ActionNone:
				r.Action, r.Severity = ActionWait, SeverityInfo
				r.Summary = fmt.Sprintf("Kubernetes node %s is back in service; the Slinky operator should undrain this node within seconds.", pod.Node)
			case kr.Action == ActionEscalateHardware || (kr.Action == ActionInvestigate && nodeFault(kr.Category)):
				r.Action, r.Severity = kr.Action, kr.Severity
			default:
				// Planned work on the Kubernetes node (a maintenance cordon, a drain
				// waiting on a PodDisruptionBudget): nothing is wrong with this node.
				// Found by drills/: k8s-pdb-drain (triage mirrored the blocked
				// drain's "investigate" onto a Slurm node that was simply busy).
				r.Action, r.Severity = ActionWait, SeverityInfo
				r.Summary = fmt.Sprintf("Drained by the Slinky operator because Kubernetes node %s is cordoned for planned work. Nothing to do on the Slurm side.", pod.Node)
				if running := a.runningJobs(); len(running) > 0 {
					r.Summary += fmt.Sprintf(" %d job(s) running here finish first.", len(running))
				}
			}
		}
	}
	r.NextSteps = []string{
		fmt.Sprintf("Work the problem on Kubernetes node %s (triage_node %s).", pod.Node, pod.Node),
		fmt.Sprintf("Once %s is fixed, uncordon it; the operator undrains %s by itself.", pod.Node, n.Name),
		"Don't resume this node in Slurm: while the Kubernetes node is cordoned, the operator re-drains it within seconds.",
	}
	r.Commands = []string{fmt.Sprintf("kubectl uncordon %s   # after the fix; the operator then undrains %s", pod.Node, n.Name)}
}

// podReplaced: the operator set the node DOWN ("slurm-operator: Pod is
// terminating") when its pod was deleted, and requeued or failed its jobs. The
// replacement pod comes up within seconds, but Slurm keeps holding the node
// DOWN: Slinky's slurm.conf has ReturnToService=0, so the new slurmd
// registering doesn't clear it, and the operator doesn't either. Slurm's own
// timestamps say which moment this is. Found by drills/: slinky-pod-kill
// (triage said "wait" and the node stayed out for minutes).
func (a assessor) podReplaced(r *Recommendation, pod slurm.SlinkyPod) {
	n := a.n
	r.Category = CategoryPodRestarted
	down, started := n.ReasonChangedAt.Time(), n.SlurmdStartTime.Time()
	if down.IsZero() || started.IsZero() || !started.After(down) {
		r.Action, r.Severity = ActionWait, SeverityInfo
		r.Summary = "Its pod is being replaced; the operator recreates it. Jobs that were running here were requeued or failed."
		r.NextSteps = []string{"Wait for the new pod's slurmd to register, then resume the node: Slurm won't do it by itself."}
		return
	}
	r.Action, r.Severity = ActionResume, SeverityInfo
	r.Summary = "Its pod was replaced and the new slurmd has registered, but Slurm still holds the node DOWN from the old pod's termination (Slinky runs ReturnToService=0). Resume it."
	r.Evidence = append(r.Evidence, fmt.Sprintf("slurmd restarted %s after the node was marked DOWN", started.Sub(down).Round(time.Second)))
	r.Commands = []string{fmt.Sprintf("kubectl -n %s exec slurm-controller-0 -c slurmctld -- scontrol update nodename=%s state=resume", pod.Namespace, n.Name)}
}

func (a assessor) hardware(r *Recommendation) {
	n := a.n
	r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
	r.Category = CategoryHardware
	r.Summary = "Hardware fault reported in the drain reason. Keep it out of service."
	// e.g. "gres/gpu count reported lower than configured (3 < 4)". After such a
	// registration the node's gres field shows the reported count, not the configured one.
	configured := slurm.GresCount(n.Gres)
	if m := gresShortfall.FindStringSubmatch(n.Reason); m != nil {
		r.Evidence = append(r.Evidence, fmt.Sprintf("slurmd registered %s %s of %s configured", m[2], m[1], m[3]))
		configured, _ = strconv.Atoi(m[3])
	}
	switch {
	case n.Has("INVALID") || n.Has("INVALID_REG") || gresShortfall.MatchString(n.Reason):
		r.Summary = "Hardware fault: the node registered with less hardware than configured. Keep it out of service."
	case strings.HasPrefix(n.Reason, "NHC:"):
		r.Summary = "Hardware fault found by the health check (NHC). Keep it out of service; NHC would resume it as soon as its check passes, so take the drain over before the repair."
	}
	r.NextSteps = []string{
		"Keep the node drained; don't resume it until the hardware is fixed.",
		"Open a hardware ticket with the reason text and node serial; on real GPUs, pull `nvidia-smi -q` and dmesg Xid lines first.",
		"After repair, run a burn-in job in the burnin partition before returning it to batch.",
	}
	r.Commands = []string{
		fmt.Sprintf("scontrol show node %s", n.Name),
		fmt.Sprintf("ssh %s 'journalctl -u slurmd -n 50; dmesg | tail -50'", n.Name),
		burnInCommand(a.p.BurninPartition, n.Name, configured) + "   # after repair",
	}
	if strings.HasPrefix(n.Reason, "NHC:") {
		// Found by drills/: nhc-gpu-lost.
		r.Commands = append([]string{fmt.Sprintf("scontrol update nodename=%s state=drain reason=\"triage: hardware: <ticket>\"   # NHC never resumes a drain it didn't set", n.Name)}, r.Commands...)
	}
}

func (a assessor) down(r *Recommendation, chronic bool) {
	n := a.n
	if chronic {
		r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
		r.Summary = "Down again; this node keeps failing. Treat it as a hardware/platform problem, not a one-off."
		r.NextSteps = []string{"Keep it out of service.", "Pull the event history into a hardware ticket.", "Burn-in before return to service."}
	} else if r.Category == CategoryUnexpectedReboot && !n.Has("NOT_RESPONDING") {
		// The node is back and slurmd is answering, but it rebooted without being
		// asked, so Slurm holds it DOWN (ReturnToService=1 only returns nodes that
		// went DOWN for not responding). Found by drills/: node-death.
		r.Action, r.Severity = ActionInvestigate, SeverityWarning
		r.Summary = "Back after rebooting without being asked (crash, power loss or watchdog); Slurm is holding it DOWN. Find out why before returning it."
		r.NextSteps = []string{
			"Read the end of the previous boot's log: a clean shutdown sequence means someone rebooted it; nothing at all means power loss or a hard hang.",
			"On real hardware, check the BMC event log (SEL) and any kernel crash dump.",
			"Resume it once the cause is understood and the health check passes. If it has happened before, escalate instead.",
		}
		r.Commands = []string{
			fmt.Sprintf("ssh %s 'journalctl -b -1 -n 30 --no-pager'", n.Name),
			fmt.Sprintf("ssh %s 'sudo nhc -t 60'", n.Name),
			fmt.Sprintf("scontrol update nodename=%s state=resume   # once the cause is understood", n.Name),
		}
		return
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
		"If it is and slurmd is down, restart it; running jobs survive a slurmd restart.",
		"If slurmd is running but still not answering, suspect authentication: a munge key that doesn't match the controller's, or a clock more than a few minutes off (munge credentials expire). Restarting slurmd won't fix either.",
		"If the host is gone, the jobs are already lost. Let the timeout mark it DOWN and follow the down-node path.",
	}
	r.Commands = []string{
		fmt.Sprintf("ping -c3 %s", n.Name),
		fmt.Sprintf("ssh %s 'systemctl is-active slurmd munge chrony; journalctl -u slurmd -n 20'", n.Name),
		fmt.Sprintf("munge -n | ssh %s unmunge   # 'Invalid credential' = key mismatch; 'Expired'/'Rewound' = clock skew", n.Name),
		fmt.Sprintf("ssh %s 'sudo systemctl restart slurmd'   # if slurmd itself is down", n.Name),
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
		if strings.HasPrefix(n.Reason, "NHC:") {
			// NHC resumes the nodes it drained itself (reason starting "NHC:") as
			// soon as every check passes, and never touches other drains.
			r.Summary = "Drained by NHC. Fix what failed; NHC returns the node to service itself at its next run once every check passes."
			r.NextSteps = []string{
				"Fix the failing condition named in the reason.",
				"Wait one health-check interval: NHC resumes the node itself when the checks pass. No manual resume needed.",
				"To keep it out regardless (you suspect hardware), re-drain it with your own reason; NHC never resumes a drain it didn't set.",
			}
			r.Commands = []string{
				fmt.Sprintf("ssh %s 'sudo nhc -t 60'   # run the checks now; resumes the node if they pass", n.Name),
				"scontrol show config | grep -i HealthCheckInterval",
			}
			break
		}
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

// reboot: someone asked Slurm to reboot the node (scontrol reboot). That's
// planned work, not an outage: REBOOT_REQUESTED drains it and waits for its
// jobs (ASAP) or for it to go idle; REBOOT_ISSUED reads as DOWN while it boots.
// If it misses ResumeTimeout, Slurm marks it DOWN ("reboot timed out") and the
// down-node path takes over. Found by drills/: maintenance-reboot.
func (a assessor) reboot(r *Recommendation) {
	n := a.n
	if r.Category == CategoryNone || r.Category == CategoryOther {
		r.Category = CategoryMaintenance
	}
	if n.Has("REBOOT_ISSUED") {
		r.Action, r.Severity = ActionWait, SeverityInfo
		r.Summary = "Rebooting on request (scontrol reboot). It rejoins by itself once slurmd registers; Slurm marks it DOWN if it misses ResumeTimeout."
		r.NextSteps = []string{"Wait for the node to come back. Investigate only if it's marked DOWN with \"reboot timed out\"."}
		r.Commands = []string{fmt.Sprintf("scontrol show node %s | grep -E 'State|Reason|BootTime'", n.Name)}
		return
	}
	if len(a.runningJobs()) > 0 {
		a.drain(r, false) // the same job checks as any drain: bounded jobs mean wait, unbounded ones block the reboot
		r.Summary = "Reboot requested; " + strings.ToLower(r.Summary[:1]) + r.Summary[1:]
	} else {
		r.Action, r.Severity = ActionWait, SeverityInfo
		r.Summary = "Reboot requested; the node reboots as soon as slurmd acts on it."
	}
	r.NextSteps = append(r.NextSteps, "Nothing else to do: Slurm drains it, reboots it, and with nextstate=RESUME returns it to service.")
	r.Commands = append(r.Commands, fmt.Sprintf("scontrol cancel_reboot %s   # only to call the reboot off", n.Name))
}

// burnInCommand submits the burn-in job: the whole node and every GPU, the
// node's own copy of the script (--wrap), output kept on the node (it runs as
// root, and root can't write the root_squash share).
func burnInCommand(partition, node string, gpus int) string {
	return fmt.Sprintf("sbatch -p %s -w %s --gres=gpu:%d --exclusive -J burnin -o /var/tmp/burnin-%%j.out --wrap /usr/local/sbin/lab-burnin",
		partition, node, max(gpus, 1))
}

// inQualification: the node is in the burn-in partition but not in production.
func (a assessor) inQualification() bool {
	return slices.Contains(a.n.Partitions, a.p.BurninPartition) && !slices.Contains(a.n.Partitions, a.p.ProductionPartition)
}

// qualify: a new or repaired node earns its way into production by passing
// burn-in: a job named "burnin" in the burn-in partition, run since the node
// last booted. Found by drills/: node-bringup (triage called a node that had
// never run a job "healthy, leave it alone").
func (a assessor) qualify(r *Recommendation) {
	n := a.n
	r.Category = CategoryQualification
	r.Evidence = append(r.Evidence, fmt.Sprintf("in partition %s but not %s: not in production yet", a.p.BurninPartition, a.p.ProductionPartition))
	burnCmd := burnInCommand(a.p.BurninPartition, n.Name, slurm.GresCount(n.Gres))
	for _, j := range a.runningJobs() {
		if j.Name == "burnin" {
			r.Action, r.Severity = ActionWait, SeverityInfo
			r.Summary = fmt.Sprintf("Burn-in job %d is running.", j.ID)
			return
		}
	}
	var last *slurm.HistoricalJob
	for i, j := range a.s.History {
		if j.Name == "burnin" && j.Partition == a.p.BurninPartition && j.OnNode(n.Name) && j.Time.End > 0 && (last == nil || j.Time.End > last.Time.End) {
			last = &a.s.History[i]
		}
	}
	booted := n.BootTime.Time()
	switch {
	case a.s.AccountingError != "":
		r.Action, r.Severity = ActionInvestigate, SeverityWarning
		r.Summary = "Not in production yet, and accounting is unavailable, so its burn-in result can't be checked."
	case last == nil || (!booted.IsZero() && time.Unix(last.Time.End, 0).Before(booted)):
		r.Action, r.Severity = ActionBurnIn, SeverityInfo
		r.Summary = "Not in production yet and no burn-in since it last booted. Run the burn-in job."
		r.Commands = []string{burnCmd}
	case !last.Succeeded():
		r.Action, r.Severity = ActionEscalateHardware, SeverityCritical
		r.Summary = fmt.Sprintf("Burn-in failed: job %d ended %s (exit %d). Keep it out of production.", last.ID, strings.Join(last.State.Current, "+"), last.ExitCode.ReturnCode.Number)
		r.Evidence = append(r.Evidence, fmt.Sprintf("burn-in job %d ended %s", last.ID, time.Unix(last.Time.End, 0).Format(time.RFC3339)))
		r.NextSteps = []string{"Read the burn-in output for the failing check.", "Fix or replace the part, reboot, and burn in again."}
		r.Commands = []string{burnCmd + "   # after the repair"}
	default:
		r.Action, r.Severity = ActionPromote, SeverityInfo
		r.Summary = fmt.Sprintf("Burn-in passed (job %d). Promote it to production.", last.ID)
		r.Evidence = append(r.Evidence, fmt.Sprintf("burn-in job %d completed with exit 0 at %s, after the node last booted", last.ID, time.Unix(last.Time.End, 0).Format(time.RFC3339)))
		r.NextSteps = []string{
			fmt.Sprintf("Add it to the %s partition. In this lab: set its stage to \"production\" in the Terraform node table, apply, and run site.yml --limit slurm.", a.p.ProductionPartition),
			"Check that the first production job lands and completes.",
		}
	}
}

// kubeNodeTrouble returns the assessment of the Kubernetes node a Slinky node
// runs on, if that node has a problem of its own (not just a cordon).
func (a assessor) kubeNodeTrouble(kubeNode string) (Recommendation, bool) {
	if a.s.Kube == nil {
		return Recommendation{}, false
	}
	kr, err := AssessKube(*a.s.Kube, kubeNode, a.p)
	if err != nil || !nodeFault(kr.Category) || (kr.Action != ActionInvestigate && kr.Action != ActionEscalateHardware) {
		return Recommendation{}, false
	}
	return kr, true
}

// nodeFault reports whether a Kubernetes recommendation is about the node
// itself being unhealthy, as opposed to planned work on it.
func nodeFault(c Category) bool {
	switch c {
	case CategoryHardware, CategoryUnreachable, CategoryKubelet, CategoryStuckProcess, CategoryResourcePressure, CategoryKernelEvents:
		return true
	}
	return false
}

// onTroubledKubeNode: Slurm sees the node as fine, but the Kubernetes node its
// pod runs on is in trouble (kubelet gone, hardware fault). The pod, and every
// job in it, lives or dies with that node. Found by drills/: k8s-kubelet-stop
// (triage said "healthy" while Kubernetes was about to evict the pod).
func (a assessor) onTroubledKubeNode(r *Recommendation, kr Recommendation) {
	pod, _ := a.n.SlinkyPod()
	r.Action, r.Severity, r.Category = ActionInvestigate, kr.Severity, CategoryKubernetesNode
	if kr.Action == ActionEscalateHardware {
		r.Action = ActionEscalateHardware
	}
	r.Summary = fmt.Sprintf("Slurm still sees this node as fine, but its Kubernetes node %s is in trouble. Its pod, and any job running in it, depends on that node.", pod.Node)
	if running := a.runningJobs(); len(running) > 0 {
		r.Summary += fmt.Sprintf(" %d job(s) are running here.", len(running))
	}
	r.NextSteps = []string{
		fmt.Sprintf("Work the Kubernetes node first (triage_node %s).", pod.Node),
		"If Kubernetes evicts the pod or the node dies, jobs running here end NODE_FAIL or are requeued. Warn their owners, or requeue the requeue-safe ones now.",
	}
	r.Commands = []string{
		fmt.Sprintf("kubectl get node %s; kubectl -n %s get pod %s -o wide", pod.Node, pod.Namespace, pod.PodName),
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

// incidentGap: drain/down events closer together than this are one incident.
const incidentGap = 10 * time.Minute

// incidents counts this node's drain/down incidents in the event window. One
// outage often logs several events ("Not responding", then "Node unexpectedly
// rebooted" when it comes back), and planned maintenance isn't a failure.
// Found by drills/: nhc-disk-full (one power loss counted twice, plus the
// current drain, made a node "chronic").
func (a assessor) incidents() int {
	evs := slices.Clone(a.events())
	slices.SortFunc(evs, func(x, y slurm.Event) int { return x.Start.Compare(y.Start) })
	n := 0
	var lastEnd time.Time
	for _, e := range evs {
		if Classify(e.Reason) == CategoryMaintenance {
			continue
		}
		if n == 0 || e.Start.Sub(lastEnd) > incidentGap {
			n++
		}
		end := e.End
		if end.IsZero() {
			end = e.Start
		}
		lastEnd = maxTime(lastEnd, end)
	}
	return n
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
