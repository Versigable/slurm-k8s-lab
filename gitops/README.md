# gitops/

Everything Argo CD manages on the lab cluster. Ansible only bootstraps what has
to exist before Argo CD can run (kubeadm, Cilium) and then Argo CD itself plus
the root Application (`ansible/roles/k8s_argocd`).

```
apps/          one Argo CD Application per add-on; the root app ("lab-root") syncs this directory
lb-pool/                Cilium LoadBalancer IP pool + L2 announcement policy (10.0.5.140-149)
local-path/             local-path-provisioner, pinned, patched to be the default StorageClass
node-problem-detector/  upstream v1.36.0 manifests + metrics patch + PodMonitor
```

Helm-chart apps keep their values inline in `apps/`: `gitlab-runner`, and
`kube-prometheus-stack` (Prometheus, Alertmanager, Grafana, node-exporter,
kube-state-metrics; lab-sized, 3-day retention on local-path).

Secrets never live in git; they're created out of band by one-time scripts:
`scripts/register-runner.sh` (runner token), `scripts/register-argocd-repo.sh`
(read-only deploy token), `scripts/create-grafana-admin.sh` (random Grafana admin password).

To change an add-on: edit it here, open an MR, let CI pass, merge. Argo CD syncs
`main` automatically, and `selfHeal` reverts manual changes made with kubectl.

Applications have no deletion finalizer on purpose: removing one from `apps/`
deletes the Application object but leaves its workloads running (orphaned),
so a bad commit can't tear down the CI runner.
