# gitops/

Everything Argo CD manages on the lab cluster. Ansible only bootstraps what has
to exist before Argo CD can run (kubeadm, Cilium) and then Argo CD itself plus
the root Application (`ansible/roles/k8s_argocd`).

```
apps/          one Argo CD Application per add-on; the root app ("lab-root") syncs this directory
lb-pool/                Cilium LoadBalancer IP pool + L2 announcement policy (10.0.5.140-149)
local-path/             local-path-provisioner, pinned, patched to be the default StorageClass
node-problem-detector/  upstream v1.36.0 manifests + metrics patch + PodMonitor
node-triage/            ServiceAccount + RBAC for the node-triage MCP server (read; patch nodes only for cordon)
```

Slurm on Kubernetes (Slinky), all Helm apps in `apps/`:

```
cert-manager          webhook certificates for the operator (its no-cert-manager fallback drifts under Argo CD)
slinky-oci-repo       Argo CD repository entry for ghcr.io/slinkyproject/charts (public OCI, no credentials)
slurm-operator-crds   the CRDs, separate so an operator change can't delete them (never pruned)
slurm-operator        the operator; cordoning a K8s node drains its Slurm node, with NPD's condition as the reason
slurm                 the cluster: slurmctld + 2 slurmd pods + slurmrestd, metrics to Prometheus
```

Helm-chart apps keep their values inline in `apps/`: `gitlab-runner`, and
`kube-prometheus-stack` (Prometheus, Alertmanager, Grafana, node-exporter,
kube-state-metrics; lab-sized, 3-day retention on local-path).

Secrets never live in git; they're created out of band by one-time scripts:
`scripts/register-runner.sh` (runner token), `scripts/register-argocd-repo.sh`
(read-only deploy token), `scripts/create-grafana-admin.sh` (random Grafana admin password),
`scripts/create-slurm-auth.sh` (Slinky's Slurm auth + JWT keys: the chart would mint new random
keys on every Argo CD render, onto immutable Secrets).

To change an add-on: edit it here, open an MR, let CI pass, merge. Argo CD syncs
`main` automatically, and `selfHeal` reverts manual changes made with kubectl.

Applications have no deletion finalizer on purpose: removing one from `apps/`
deletes the Application object but leaves its workloads running (orphaned),
so a bad commit can't tear down the CI runner.
