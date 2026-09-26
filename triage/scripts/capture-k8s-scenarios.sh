#!/usr/bin/env bash
# Break the lab's Kubernetes cluster in known ways and capture what the API
# reports, so node-triage's Kubernetes rules are tested against real output.
# Every scenario restores the cluster before the next one.
#
#   ./capture-k8s-scenarios.sh                        # all scenarios
#   ./capture-k8s-scenarios.sh readonly-fs notready   # just these
#
# Kernel faults are injected the way node-problem-detector itself is tested:
# a matching line written to /dev/kmsg on the node.
set -euo pipefail

CP=${CP:-labadmin@10.0.5.133}
W1=${W1:-labadmin@10.0.5.134}
W2=${W2:-labadmin@10.0.5.135}
KEY=${KEY:-$HOME/.ssh/slurm-lab}
OUT=${OUT:-$(cd "$(dirname "$0")/.." && pwd)/internal/kube/testdata}

on() { local host=$1; shift; ssh -n -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$host" "$@"; }
log() { printf '\n==> %s\n' "$*"; }

# Trim API output to the fields triage reads: the repo is public, and full pod
# specs and node image lists are noise (and could carry env values).
TRIM='
import json, sys
kind = sys.argv[1]
d = json.load(sys.stdin)
keep_meta = lambda m, extra=(): {k: m[k] for k in ("name", "namespace", "labels", "ownerReferences", *extra) if k in m}
out = []
for o in d["items"]:
    if kind == "nodes":
        ann = {k: v for k, v in o["metadata"].get("annotations", {}).items() if k.startswith("slurm-k8s-lab/")}
        m = keep_meta(o["metadata"]); m["annotations"] = ann
        st = o["status"]
        out.append({"metadata": m, "spec": {k: o["spec"][k] for k in ("unschedulable", "taints") if k in o["spec"]},
                    "status": {"conditions": st.get("conditions", []), "nodeInfo": {"kubeletVersion": st["nodeInfo"]["kubeletVersion"]}}})
    elif kind == "pods":
        ann = {k: v for k, v in o["metadata"].get("annotations", {}).items() if k == "kubernetes.io/config.mirror"}
        m = keep_meta(o["metadata"]); m["annotations"] = ann
        out.append({"metadata": m, "spec": {"nodeName": o["spec"].get("nodeName", "")}, "status": {"phase": o["status"].get("phase", "")}})
    else:
        out.append(o)
json.dump({"items": out}, sys.stdout, indent=1)
'

capture() {
  local dir="$OUT/$1"
  mkdir -p "$dir"
  on "$CP" "kubectl get nodes -o json | python3 -c '$TRIM' nodes" > "$dir/nodes.json"
  on "$CP" "kubectl get pods -A -o json | python3 -c '$TRIM' pods" > "$dir/pods.json"
  on "$CP" "kubectl get events -A --field-selector involvedObject.kind=Node -o json | python3 -c '$TRIM' events" > "$dir/events.json"
  on "$CP" "kubectl get pdb -A -o json | python3 -c '$TRIM' pdbs" > "$dir/pdbs.json"
  on "$CP" 'date +%s' > "$dir/now.txt"
  on "$CP" 'kubectl get nodes -o custom-columns=NODE:.metadata.name,SCHED:.spec.unschedulable,READY:".status.conditions[?(@.type==\"Ready\")].status"' | sed "s/^/    /"
  log "captured $1"
}

# node, condition type, wanted status, timeout
wait_condition() {
  local deadline=$((SECONDS + $4))
  until [[ $(on "$CP" "kubectl get node $1 -o jsonpath='{.status.conditions[?(@.type==\"$2\")].status}'") == "$3" ]]; do
    ((SECONDS < deadline)) || { echo "timed out waiting for $1 $2=$3" >&2; return 1; }
    sleep 3
  done
}

INJECTED_AT=0
kmsg() { on "$1" "echo '$2' | sudo tee /dev/kmsg >/dev/null"; INJECTED_AT=$SECONDS; }

# NPD's kernel monitor replays /dev/kmsg over its lookback window (5m in the
# upstream config) when it starts. Restarting it sooner re-reads the injected
# line and re-sets the condition, so wait the window out first. (Operationally:
# a permanent NPD condition survives an NPD restart until the triggering kernel
# line is older than the lookback; a reboot clears it.)
NPD_LOOKBACK=${NPD_LOOKBACK:-300}
reset_npd() {
  local wait=$((INJECTED_AT + NPD_LOOKBACK + 15 - SECONDS))
  if ((wait > 0)); then echo "    waiting ${wait}s for the kmsg line to leave NPD's lookback window"; sleep "$wait"; fi
  on "$CP" "kubectl -n kube-system delete pod -l app=node-problem-detector --field-selector spec.nodeName=$1 --wait=true >/dev/null"
  on "$CP" "kubectl -n kube-system rollout status ds/node-problem-detector --timeout=120s >/dev/null"
}

# Clears a leftover NPD condition on a node (e.g. after an interrupted run):
# waits a full lookback window, then restarts NPD there.
scenario_reset_w1() {
  log "reset: k8s-w1 NPD conditions"
  INJECTED_AT=$SECONDS
  reset_npd k8s-w1
  wait_condition k8s-w1 ReadonlyFilesystem False 60
  wait_condition k8s-w1 KernelDeadlock False 60
}

scenario_healthy() {
  log "scenario: healthy"
  capture healthy
}

scenario_readonly_fs() {
  log "scenario: readonly-fs (ext4 remounts the root fs read-only on k8s-w1)"
  kmsg "$W1" "EXT4-fs (vda1): Remounting filesystem read-only"
  wait_condition k8s-w1 ReadonlyFilesystem True 60
  capture readonly-fs
  reset_npd k8s-w1
  wait_condition k8s-w1 ReadonlyFilesystem False 60
}

scenario_kernel_deadlock() {
  log "scenario: kernel-deadlock (hung task on k8s-w1)"
  kmsg "$W1" "task docker:4242 blocked for more than 120 seconds."
  wait_condition k8s-w1 KernelDeadlock True 60
  capture kernel-deadlock
  reset_npd k8s-w1
  wait_condition k8s-w1 KernelDeadlock False 60
}

scenario_cordoned_pdb() {
  log "scenario: cordoned-pdb (k8s-w2 cordoned for maintenance; a PDB blocks its drain)"
  on "$CP" 'kubectl create namespace triage-drill --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n triage-drill apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: checkout-api}
spec:
  replicas: 2
  selector: {matchLabels: {app: checkout-api}}
  template:
    metadata: {labels: {app: checkout-api}}
    spec:
      nodeSelector: {kubernetes.io/hostname: k8s-w2}
      containers: [{name: web, image: "nginx:stable-alpine"}]
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: checkout-api}
spec:
  minAvailable: 2
  selector: {matchLabels: {app: checkout-api}}
YAML
kubectl -n triage-drill rollout status deploy/checkout-api --timeout=120s >/dev/null
kubectl cordon k8s-w2 >/dev/null
kubectl annotate node k8s-w2 --overwrite "slurm-k8s-lab/triage-reason=maint: kernel update CHG-2044" >/dev/null'
  sleep 5
  capture cordoned-pdb
  on "$CP" 'kubectl uncordon k8s-w2 >/dev/null; kubectl annotate node k8s-w2 slurm-k8s-lab/triage-reason- >/dev/null; kubectl delete namespace triage-drill --wait=true >/dev/null'
}

scenario_notready() {
  log "scenario: notready (kubelet stopped on k8s-w2; captured before pod eviction)"
  on "$W2" 'sudo systemctl stop kubelet'
  wait_condition k8s-w2 Ready Unknown 120
  sleep 5 # let the node lifecycle controller add the unreachable taints
  capture notready
  on "$W2" 'sudo systemctl start kubelet'
  wait_condition k8s-w2 Ready True 120
}

scenario_task_hung_events() {
  log "scenario: task-hung-events (3 hung-task kernel events on an otherwise healthy k8s-w2)"
  for pid in 117 118 119; do
    kmsg "$W2" "task kworker/u8:2:$pid blocked for more than 120 seconds."
    sleep 2
  done
  sleep 10
  capture task-hung-events
}

scenarios=("$@")
[[ ${#scenarios[@]} -gt 0 ]] || scenarios=(healthy readonly-fs kernel-deadlock cordoned-pdb notready task-hung-events)
for s in "${scenarios[@]}"; do
  "scenario_${s//-/_}"
done

log "final state"
on "$CP" 'kubectl get nodes'
