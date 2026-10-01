# Failure drills

Each drill breaks one thing on purpose, the way it breaks in a real fleet, and
keeps a timeline:

| Step | What happens |
|---|---|
| **inject** | the fault (or the planned change) goes in |
| **detect** | the scheduler notices |
| **decide** | node-triage is asked what to do, over MCP and read-only, exactly as an on-call agent asks it; the answer is graded against what a good engineer would do |
| **fix** | the remedy goes in |
| **recover** | the node or service is back |

Then the drill undoes everything it did and waits until the **whole lab is
green** again: every Slurm node in service, every Kubernetes node Ready and
schedulable, every pod running, every Argo CD app Synced/Healthy, and
node-triage agreeing that nothing needs attention. That makes the drills safe
to run back to back, and it proves the cleanup as much as the fault.

A drill ends:

- **PASS**: everything went as expected, including node-triage's decisions.
- **GAP**: the lab recovered, but node-triage's decision at some point wasn't
  the right one. That's a finding: the fix goes into node-triage, with the
  drill's captured state as the regression test, and the drill is run again.
- **FAIL**: the drill itself didn't go as designed, or the lab didn't get back
  to green.

Results: [SCORECARD.md](SCORECARD.md) (latest run of each drill) and
[results/](results/) (full timelines as JSON). What the drills found and what
changed because of it: [docs/drills.md](../docs/drills.md).

## Running

From the control node (where `scripts/rebuild.sh` runs), with `terraform` and
`ansible-playbook` on `PATH`:

```bash
drills/drill.py list                 # the catalog
drills/drill.py check                # is the lab green right now?
drills/drill.py run node-death       # one or more drills
drills/drill.py run --all            # all of them (about 1.5 hours)
drills/drill.py run --capture ...    # also save node-triage fixtures at every decision point
drills/drill.py report               # rebuild SCORECARD.md
```

A drill refuses to start unless the lab is green (`--force` overrides).
Drills that need an optional lab feature (NHC, `RebootProgram`, the node-stage
plumbing) skip themselves when it's missing.

## Safety

- Every fault is reversible, and every drill registers its cleanup before it
  injects, so an error or Ctrl-C still puts the lab back.
- Power operations use the pool-scoped Proxmox token Terraform uses: a drill
  can only ever touch lab VMs.
- Kubernetes drill workloads live in their own `drill` namespace, which Argo CD
  doesn't manage, so self-heal never fights a drill.
- Nothing here needs credentials beyond what `rebuild.sh` already uses, and no
  secret is ever put on a command line.

## Writing a drill

Drills live in [catalog.py](catalog.py). A drill is a function with a
`@drill(...)` decorator that receives a `Run`:

```python
@drill("my-drill", "One-line title", minutes=5, teaches="What it shows.")
def my_drill(r):
    r.cleanup("undo the fault", lambda: ...)           # always first
    ...                                                # inject
    r.mark("inject", "what went in")
    r.wait("scheduler notices", lambda: ..., 300, kind="detect")
    r.decide("node", ["investigate"], "label")        # grade node-triage
    ...                                                # fix
    r.mark("fix", "what fixed it")
    r.wait("back in service", lambda: ..., 300, kind="recover")
```

Expectations in `r.decide()` are what a good compute-production engineer would
do at that moment, not what node-triage happened to say when the drill was
written.
