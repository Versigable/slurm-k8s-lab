# Classic Slurm vs Slurm on Kubernetes (Slinky)

This lab runs the same scheduler twice:

- **Classic:** Slurm on three VMs (slurm-ctl, slurm-c1, slurm-c2), configured by Ansible.
- **Slinky:** Slurm on the kubeadm cluster through SchedMD's slurm-operator 1.2.2 (slurmctld, 2 slurmd pods, slurmrestd), synced by Argo CD.

Everything below was observed on these two clusters. Anything not exercised in the lab says so.

## At a glance

| | Classic (VMs) | Slinky (Kubernetes) |
|---|---|---|
| Slurm version | 24.11.5, Debian 13 packages | 26.05.4, SchedMD images (`26.05-ubuntu26.04`) |
| Upgrade path | apt + Ansible, host by host | image tag bump in git; Argo CD syncs, operator rolls pods |
| What you declare | `slurm.conf` templated per host (`NodeName=` lines, partitions) | 3 custom resources: `Controller`, `NodeSet`, `RestApi`; the operator renders config and builds StatefulSets/Services |
| Compute nodes | Static, named after hosts | **Dynamic nodes** (`DYNAMIC_NORM`) that register themselves, addressed by pod IP (`10.244.x`) |
| Auth | munge key, copied host to host by Ansible | Slurm auth key + JWT key in Secrets (JWT for slurmrestd, which the operator itself uses) |
| Accounting | slurmdbd + MariaDB on the controller VM | Needs an external database (e.g. mariadb-operator). **Not enabled here**: `sacct` says accounting storage is disabled |
| User identity | Local users with fixed UIDs on every node | Login pods + sssd/LDAP. Without that, jobs submitted via `kubectl exec` run as `slurm` |
| GPUs | Fake GRES: char devices on an unused major, cgroup-isolated | GRES through device plugins / DRA. **Not exercised** (no GPUs in the lab) |
| Config management | Ansible (`site.yml`) | GitOps (`gitops/apps/slurm*.yaml`), plus two one-time secrets |

## What actually differed

**1. Resource accounting is the sharp edge.** Each slurmd pod reports its whole VM to Slurm (4 CPUs, 7947 MB) while Kubernetes thinks the pod uses its request (200m CPU, 256 Mi). Slurm will hand out CPU and memory that the Kubernetes scheduler considers free for other pods, so both schedulers can book the same resources. Running Slinky safely means dedicating nodes to it (taints plus a node selector) or setting slurmd requests to the node's allocatable size. Classic Slurm owns its VMs outright, so it doesn't have this problem.

**2. Node lifecycle is Kubernetes-driven.** Cordoning a Kubernetes node made the operator drain the Slurm node running there **within 1 second**. Uncordoning undrained it immediately. With `propagatedNodeConditions: [ReadonlyFilesystem, KernelDeadlock]`, the Slurm drain reason carried the node-problem-detector condition:

```
Reason=slurm-operator: (FilesystemIsReadOnly: EXT4-fs (vda1): Remounting filesystem read-only)
```

The conditions **don't trigger** the drain on their own; the cordon does (confirmed in `nodeset_sync.go`). Something has to act on the condition. Here it was the node-triage MCP server, whose `drain_node` cordons on Kubernetes. On the classic cluster the equivalent is `scontrol update state=drain` by a person or a tool.

**3. Failure modes move.** On classic Slurm, a dead slurmd leaves jobs running (`slurmstepd` survives) until `SlurmdTimeout` marks the node DOWN, and a dead host requeues its batch jobs. On Slinky, a crashed slurmd is a pod restart (StatefulSet), and host loss becomes Kubernetes' problem first (NotReady, taints, eviction after 300 s). `workloadDisruptionProtection` adds a PodDisruptionBudget over worker pods with running jobs, so a routine `kubectl drain` waits for jobs instead of killing them.

**4. GitOps needs care with Slurm's secrets.** The slurm chart creates its auth and JWT keys with Helm `lookup` + random data on **immutable** Secrets. Argo CD renders charts without `lookup`, so every sync would mint a new key (and fail on the immutable Secret). The operator chart's webhook certificates have the same problem without cert-manager. Both are handled here: the keys are created once, out of band (`scripts/create-slurm-auth.sh`), and cert-manager issues the webhook certs.

## When I'd pick which

- **Classic** for a dedicated GPU training fleet with long jobs and an HPC ops team: Slurm owns the hardware outright, nodes are cattle only at the hardware level, and accounting and identity are already solved.
- **Slinky** when the platform is already Kubernetes and researchers want the Slurm interface: node lifecycle, upgrades and config flow through the same GitOps and node-health machinery as everything else. It needs dedicated nodes (point 1), an external accounting database, and an identity service before it's production-ready.
