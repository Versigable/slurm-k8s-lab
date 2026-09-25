# AGENTS.md

Guidance for coding agents (and humans) working in this repo.

## Layout

- `terraform/`: Proxmox VMs. One node map in `main.tf` is the source of truth; it also generates `ansible/inventory/hosts.yml`.
- `ansible/`: node configuration. `site.yml` converges the cluster, `verify.yml` proves it works, `triage.yml` deploys the MCP server.
- `triage/`: Go module for the `node-triage` MCP server.
  - `internal/slurm`: CLI JSON parsing (Slurm 24.11, data_parser v0.0.42), fixture runner.
  - `internal/triage`: the decision rules. Pure functions of a `Snapshot`; no I/O.
  - `internal/server`: MCP tool wiring.
  - `internal/slurm/testdata/<scenario>/`: **real** captures from the lab cluster.

## Rules

- **Never hand-write Slurm JSON fixtures.** Add a scenario to `triage/scripts/capture-scenarios.sh`, break the lab on purpose, and capture what Slurm actually reports. Synthetic `Snapshot` values in tests are fine for rules the lab can't reproduce yet; say so in the test.
- **Triage rules stay deterministic.** Same snapshot, same recommendation. No LLM calls, clocks or randomness inside `internal/triage`. `Snapshot.Now` is an input.
- **Every recommendation carries its evidence.** If a rule can't point at a Slurm fact, it shouldn't fire.
- **Write tools stay opt-in.** `drain_node`/`resume_node` register only with `-allow-writes`. Don't add a write path that bypasses the hardware-fault guard in `resume_node`.
- **No secrets in the repo.** It's mirrored publicly. Tokens live in the environment; generated secrets go to `ansible/.secrets/` (gitignored).

## Checks

CI runs on every merge request and on `main` (`.gitlab-ci.yml`): gofmt + `go vet` + `go test`, `ansible-lint` (production profile), `terraform fmt`/`validate`, `shellcheck`, and a Python compile check. `main` only accepts merges whose pipeline passed.

Jobs run as pods on the lab's own Kubernetes cluster (GitLab Runner, Kubernetes executor, tag `k8s-lab`; see `ansible/ci-runner.yml`). Every job must carry that tag: other runners visible to this project are production host runners.

Run the same checks locally before pushing:

```bash
cd triage && gofmt -l . && go vet ./... && go test ./...
cd ansible && ansible-lint site.yml verify.yml triage.yml k8s-verify.yml ci-runner.yml
cd terraform && terraform fmt -check && terraform validate
```

Commits: imperative subject, a body that says *why*.
