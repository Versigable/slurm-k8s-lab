---
name: triage-slurm-node
description: Work out what to do with an unhealthy Slurm or Kubernetes node (drained, down, cordoned, NotReady, invalid, kernel faults, or eating jobs) using the node-triage MCP server, and hand the on-call engineer a decision with evidence. Use when someone asks why a node is drained, cordoned or down, whether it can go back into service, or which nodes need attention.
---

# Triage a Slurm or Kubernetes node

You have the `node-triage` MCP server. It covers three clusters: classic Slurm (`lab`), Slurm on Kubernetes (`slinky`), and Kubernetes. Every recommendation carries `scheduler` (slurm | kubernetes) and `cluster`. Its recommendations come from deterministic rules over live Slurm state. Your job is to run the right tools, relay the decision faithfully, and stop at the approval boundary.

## Steps

1. **Scope.** If no node was named, call `triage_cluster` and work the list from the top (most severe first). If a node was named, call `triage_node` for it.
2. **Get the facts.** Call `node_detail` when you need more than the recommendation's evidence: who set the reason and when, which jobs are running, recent NODE_FAIL jobs, prior drain/down events.
3. **Report.** Lead with the action and severity, then the evidence *as the tool gave it*, then any preconditions, then the suggested commands. Don't soften `escalate_hardware` into "might be worth a look".
4. **Stop at the boundary.** The read-only tools never change the cluster. Only call `drain_node` or `resume_node` when:
   - the server was started with writes enabled (the tools exist), **and**
   - the engineer explicitly approved that action for that node in this conversation, **and**
   - for a resume, every listed precondition has been confirmed by a person.
   Never pass `force=true` to `resume_node` on your own initiative. It overrides a hardware-fault refusal and exists for after the repair and burn-in.
5. **Confirm.** Only say a node was drained or resumed if the tool returned `done: true`. Quote the `command` it ran.

## How to read the actions

| Action | Meaning | Typical next move |
|---|---|---|
| `none` | Healthy | Nothing |
| `wait` | Draining; jobs still running | Wait, unless a job has no time limit (then talk to its owner) |
| `drain` | Healthy-looking but eating jobs | Drain with the suggested reason, then investigate |
| `resume` | Drained for a known, finished reason | Confirm preconditions, then resume |
| `investigate` | Needs a human look first | Follow `next_steps`; don't resume yet |
| `escalate_hardware` | Hardware fault, failed burn-in, or chronic node | Keep it out; hardware ticket; burn-in before return |
| `burn_in` | New or repaired node, not in production yet | Run the suggested burn-in job |
| `promote` | Burn-in passed since the node last booted | Add it to production (in this lab: its stage in the Terraform node table) |

## Slurm vs Kubernetes

- `drain_node` on Slurm drains (running jobs finish, nothing new starts). On Kubernetes it **cordons** (running pods stay, nothing new is scheduled). Evicting pods (`kubectl drain`) is left to the engineer: say so, and include the suggested command.
- A Kubernetes node that is cordoned but whose drain is **blocked by a PodDisruptionBudget** needs a person: capacity elsewhere or an agreed disruption. Never suggest deleting pods to get past a PDB.
- **Slinky nodes follow their Kubernetes node.** A Slinky node drained with a reason starting `slurm-operator:` was drained because its Kubernetes node is cordoned (category `kubernetes_managed`). Don't resume it in Slurm: the operator re-drains it within seconds, and `resume_node` refuses. Work the Kubernetes node (its name is in the evidence) and uncordon *that* once fixed.
- **Slinky pod replaced** (category `pod_restarted`): the operator marks a node DOWN while its pod terminates. `wait` until the new slurmd registers, then `resume` is safe (and `resume_node` allows it).
- **Slinky node on a troubled Kubernetes node** (category `kubernetes_node`): Slurm still sees it as fine, but its pod lives or dies with that Kubernetes node. Work the Kubernetes node first.
- `ReadonlyFilesystem` and `KernelDeadlock` come from node-problem-detector reading the kernel log. They clear on reboot, or when NPD restarts after the triggering line is older than its 5-minute lookback. Restarting NPD sooner just re-detects the fault.

## When a whole component is unreachable

If triage couldn't read part of a cluster, `triage_cluster` lists it in `unavailable` and ranks a recommendation for it alongside the nodes, named after the component: `slurmctld` or `slurmrestd` (the cluster's node and job state), `slurmdbd` (accounting only: history-based checks are off, node state is still live), or `kube-apiserver`. Everything else in the list was still triaged. Report the outage first; for `slurmrestd`, the evidence says if its pods sit on a troubled Kubernetes node.

## Don't

- Don't invent causes the evidence doesn't show. "Unreachable" means the controller lost slurmd; it doesn't tell you *why*.
- Don't run the suggested commands yourself through some other tool. They're for the engineer.
- Don't treat a quiet `triage_cluster` as proof the cluster is fine beyond what Slurm can see (it doesn't read hardware sensors or GPU telemetry), or while `unavailable` isn't empty.
