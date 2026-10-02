# What the failure drills found

The drills in [`drills/`](../drills/README.md) break the lab on purpose (one fault
at a time), time how long the schedulers take to notice and recover, and ask
node-triage what to do at each decision point, exactly as an on-call agent
would. This page is what they found on their first runs (2026-10-01) and what
changed because of it. The latest numbers are in
[drills/SCORECARD.md](../drills/SCORECARD.md).

Every finding below was observed on the lab, not reasoned out in advance. Where
a drill's own expectation turned out to be wrong, that's listed too: being
wrong about the system is the point of running the drill.

## Timings worth knowing

Measured on the lab: classic Slurm 24.11 (`SlurmdTimeout=300`), Slinky Slurm
26.05, Kubernetes 1.36.

| What happened | How long |
|---|---|
| Compute node loses power: until slurmctld flags it NOT_RESPONDING | 161 s |
| ...until it's marked DOWN and its jobs fail or requeue | 458 s (SlurmdTimeout counts from the last contact) |
| ...until the requeued job runs again on another node | 585 s (requeued jobs wait ~120 s, reason `BeginTime`) |
| Munge key mismatch on a node: until NOT_RESPONDING | 125 s |
| Clock 10 min ahead: until NOT_RESPONDING | 81 s |
| NHC catches a full disk or a missing GPU (60 s interval) | 40-43 s |
| NHC resumes the node after the fix | 54 s |
| `scontrol reboot ASAP nextstate=RESUME` with a job running: request to back in service | 75 s, no human step |
| slurmdbd down: queued job records replayed after restart | 22 s (17 records) |
| Kubernetes kubelet stops: until the node is Ready=Unknown | 46 s |
| ...until its pods are evicted | ~300 s more (default toleration) |
| Slinky worker pod deleted: until Slurm marks the node DOWN and requeues the job | 0.7 s |
| Slinky operator labels a busy worker pod as protected | 25 s after the job starts |

The classic/Slinky contrast is the headline: on a VM, slurmctld takes minutes to
give up on a dead node, so its jobs sit on it; in Slinky, pod termination tells
slurmctld at once, and the job is requeued in under a second.

## Findings in node-triage (fixed)

Each one was a drill decision graded GAP. The captured state at that moment is
now a regression test in `triage/internal/*/testdata/`.

1. **One failed source blinded triage for every cluster.** With slurmdbd down,
   a single failing `sacct` call made `triage_cluster` return an error for
   classic Slurm, Slinky and Kubernetes alike. Same with slurmctld down, and
   with slurmrestd unreachable. Sources are now read in parallel, each bounded
   by a timeout. A source that fails becomes a ranked `control_plane`
   recommendation naming the component (`slurmctld`, `slurmdbd`, `slurmrestd`,
   `kube-apiserver`), the rest are still triaged, and losing only accounting
   is a warning that says which checks are off.
   *(slurmdbd-outage, slurmctld-outage, k8s-kubelet-stop)*

2. **"Chronic" counted events, not incidents.** One power loss logs two events
   (`Not responding`, then `Node unexpectedly rebooted` when the node returns).
   Two of those plus the drain being assessed made a node with a full disk
   "chronic": escalate as hardware. Back-to-back events now count as one
   incident, and planned maintenance doesn't count. *(nhc-disk-full)*

3. **A planned reboot read as a failing node.** During `scontrol reboot`, the
   node is DOWN+REBOOT_ISSUED; triage took the DOWN path and, with the chronic
   miscount, said "this node keeps failing, escalate". `REBOOT_REQUESTED` and
   `REBOOT_ISSUED` are now planned work: wait. *(maintenance-reboot)*

4. **Triage didn't recognize its own drain reason.** NHC resumes the nodes it
   drained as soon as its check passes, which would put a node with a replaced
   GPU back in production without burn-in. The fix is to take the drain over
   (`triage: hardware: ...`), and NHC then leaves it alone; but triage called
   that reason "unrecognized" and dropped the hardware escalation. A `triage:`
   drain now keeps the category of what it says. Triage also now tells you to
   take the drain over when NHC finds a hardware fault. *(nhc-gpu-lost)*

5. **A node back from an unexpected reboot was "unreachable".** Slurm holds it
   DOWN (`ReturnToService=1` only returns nodes that went DOWN for not
   responding), which is right: a surprise reboot is a hardware signal. But
   triage said "slurmd is unreachable" about a node that was answering. It now
   says find out why it rebooted (previous boot's log, BMC events), then
   resume. *(node-death)*

6. **Slinky: "wait" for a node that would never come back.** Deleting or
   evicting a worker pod makes the operator mark the Slurm node DOWN
   (`slurm-operator: Pod is terminating`). The replacement pod registers within
   seconds, but Slurm holds the node DOWN. Triage assumed the operator would
   undrain it and said wait; it stayed out for minutes. Triage now compares
   Slurm's own timestamps: once the new slurmd registered after the node went
   DOWN, it says resume. The platform fix is below. *(slinky-pod-kill)*

7. **Slinky: a busy node on a maintenance drain got "investigate".** With
   k8s-w1 cordoned and its drain waiting on PodDisruptionBudgets, triage
   mirrored the Kubernetes node's "investigate" onto the Slurm node, which was
   simply running a job. Only real node faults (hardware, kubelet, kernel,
   pressure) are mirrored now; planned work means wait. *(k8s-pdb-drain)*

8. **Slinky's control plane vanished without a trace.** With the kubelet down
   on k8s-w2, Kubernetes marks that node's pods NotReady and drops them from
   their Services within a minute, so slurmrestd became unreachable although
   its container was still running. Triage now traces a slurmrestd outage to
   the troubled Kubernetes node its pods run on. A Slinky node whose own
   Kubernetes node is in trouble also inherits that, even while Slurm still
   sees it as fine. *(k8s-kubelet-stop, k8s-kubelet-stop-worker)*

9. **Advice that wouldn't have worked.** For a node that stopped responding
   because of a munge key mismatch or clock skew, triage's next step was
   "restart slurmd", while slurmd was running fine. The steps now say to
   suspect authentication when slurmd is up, with the one test that tells:
   `munge -n | ssh <node> unmunge` ("Invalid credential" = key, "Expired" or
   "Rewound" = clock). *(munge-key-mismatch, clock-skew)*

10. **New nodes looked healthy.** A node in the burn-in partition that had never
    run a job was "healthy, leave it alone". Triage now runs a qualification
    rule: `burn_in` until a burn-in job passes since the node last booted, then
    `promote`, or `escalate_hardware` if it failed. *(node-bringup)*

## Findings in the platform (fixed)

- **Slinky left nodes DOWN after every pod replacement**, a Kubernetes drain
  included: Slurm's default `ReturnToService=0`. Slinky now runs
  `ReturnToService=2` (via the chart's `extraConfMap`): a DOWN node returns once
  its new slurmd registers. It never clears a DRAIN, and node faults reach Slinky
  as operator drains, so faults still keep nodes out.
- **NHC could detect but never drain.** On Debian, nhc looks for its helpers in
  `/usr/lib/nhc`; they were installed under `/usr/libexec`. And nhc runs checks
  with globbing off, so the GPU check counted 0 of 4 devices on healthy nodes.
  The site.yml check caught the second; it now also checks the helpers exist.
- **The burn-in had never run as a job.** `sbatch` reads the script on the
  submit host (it's installed on compute nodes: use `--wrap`), and root can't
  write the root_squash share, so the job died before it started (signal 53).
- **No `crictl` on the Kubernetes nodes**, the tool for inspecting static pods
  when the API server is down. Installed.

## Findings about the platform (not changed)

- **Node-local volumes pin state.** Prometheus (on k8s-w1) and Slinky's
  slurmctld state (on k8s-w2) live on local-path volumes. Draining either node
  stops that service for the whole maintenance window, and a node that dies
  takes it with it. With the kubelet gone, slurm-controller-0 couldn't be
  replaced at all (a StatefulSet pod isn't recreated until the old one is
  confirmed gone). A real cluster needs replicated storage or a controller
  placement policy; the lab accepts it.
- **One slurmd per Kubernetes node.** An evicted Slinky worker can't move to
  the other worker, so a drain halves Slinky's capacity until the node returns.
- **Slinky protects busy workers, after a delay.** Its PodDisruptionBudget kept
  `kubectl drain` waiting until the Slurm job finished, then evicted the idle
  pod: no job lost. But the protection label appeared 25 s after the job
  started.

## Where the drills were wrong

- **node-death** expected the node to rejoin by itself after power returned
  (`ReturnToService=1`). It doesn't after an unexpected reboot; see finding 5.
- **k8s-cert-renewal** expected kube-apiserver to keep serving the old
  certificate until restarted. It reloads its serving certificate by itself;
  the restart is for the client certificates in the components' kubeconfigs.
  It also assumed `crictl` was installed.
- **slinky-pod-kill** expected "wait" right after the pod was deleted. The
  replacement had registered within 7 s, so "resume" was already the right
  answer.
