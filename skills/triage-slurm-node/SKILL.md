---
name: triage-slurm-node
description: Work out what to do with an unhealthy Slurm node (drained, down, invalid, or eating jobs) using the node-triage MCP server, and hand the on-call engineer a decision with evidence. Use when someone asks why a node is drained or down, whether it can go back into service, or which nodes need attention.
---

# Triage a Slurm node

You have the `slurm-node-triage` MCP server. Its recommendations come from deterministic rules over live Slurm state. Your job is to run the right tools, relay the decision faithfully, and stop at the approval boundary.

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
| `escalate_hardware` | Hardware fault or chronic node | Keep it out; hardware ticket; burn-in before return |

## Don't

- Don't invent causes the evidence doesn't show. "Unreachable" means the controller lost slurmd; it doesn't tell you *why*.
- Don't run the suggested commands yourself through some other tool. They're for the engineer.
- Don't treat a quiet `triage_cluster` as proof the cluster is fine beyond what Slurm can see (it doesn't read hardware sensors or GPU telemetry).
