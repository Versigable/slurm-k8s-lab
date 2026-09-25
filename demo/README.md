# node-triage demo

![node-triage demo](node-triage-demo.gif)

Claude Code (headless `claude -p`) triaging a real failure on the lab's Slurm cluster through the node-triage MCP server:

1. **A GPU falls off the bus on slurm-c1.** slurmd registers 3 of 4 GPUs; slurmctld marks the node `INVALID_REG` with `gres/gpu count reported lower than configured (3 < 4)`.
2. **"Anything in the Slurm cluster need attention?"** Claude calls `triage_cluster` and relays `escalate_hardware` with its evidence and next steps.
3. **"Resume slurm-c1, I approve."** The server's hardware guard refuses (`done: false`), and Claude does not retry with `force=true` on its own.
4. **"Drain slurm-c2 for CHG-1043, a BIOS update tonight. I approve."** `drain_node` runs (`done: true`); `sinfo` shows the node drained with a `triage:`-prefixed reason.

## How it was recorded

- Everything on screen is real: the cluster, the scheduler output, the tool calls and Claude's replies. `run_demo.py` breaks the cluster, drives three `claude -p` turns in one session, and restores the cluster afterwards.
- Claude gets **only** the slurm-triage MCP tools (`--tools ""`, `--strict-mcp-config`, `--permission-mode dontAsk`) with [`skills/triage-slurm-node/SKILL.md`](../skills/triage-slurm-node/SKILL.md) appended as its instructions. It runs from an empty directory so repo state doesn't leak into its context.
- The demo server runs with `-allow-writes`; the day-to-day Claude Code config does not.
- **Timing:** `node-triage-demo.raw.cast` has wall-clock timings. [`retime.py`](retime.py) produced `node-triage-demo.cast` by trimming model latency to 1.5 s and adding reading pauses after long answers. The text on screen is unchanged. The GIF is rendered from the retimed cast with [agg](https://github.com/asciinema/agg).
- **Runs vary.** The model isn't deterministic. Across five recorded takes, Claude called `resume_node` and hit the server's refusal four times, and once declined on its own (the skill's rule) without calling it. Both layers are real; this take shows both. The server-side guard is also covered by `TestResumeRefusesHardwareFault`.
- **Earlier takes found two bugs, both fixed before this one.** The first take's approved drain failed with `Invalid user id`: the triage user had Slurm's Operator level, which can't change node state (it now gets Admin). A later take had Claude remarking on the repo's uncommitted files, because headless Claude Code includes the working directory's git status (it now runs from an empty directory).

## Run it yourself

With the lab up and `node-triage` deployed (`ansible-playbook ansible/triage.yml`):

```bash
python demo/run_demo.py --key ~/.ssh/<key authorized for labadmin>          # writes demo/node-triage-demo.raw.cast
python demo/retime.py demo/node-triage-demo.raw.cast demo/node-triage-demo.cast --height 48
agg --font-family Consolas --idle-time-limit 12 --last-frame-duration 6 demo/node-triage-demo.cast demo/node-triage-demo.gif
asciinema play demo/node-triage-demo.cast   # or play it in a terminal
```

The demo leaves the cluster as it found it (both computes `IDLE`).
