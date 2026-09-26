#!/usr/bin/env bash
# Break the lab cluster in known ways and capture what Slurm reports, so the
# triage tests run against real scheduler output instead of hand-written JSON.
#
# Run from the control node. Every scenario restores the cluster before the next one.
#   ./capture-scenarios.sh                      # all scenarios
#   ./capture-scenarios.sh gres-missing node-down  # just these
# Hosts default to the lab IPs; override with CTL=, C1=, C2=.
set -euo pipefail

CTL=${CTL:-labadmin@10.0.5.130}
C1=${C1:-labadmin@10.0.5.131}
C2=${C2:-labadmin@10.0.5.132}
KEY=${KEY:-$HOME/.ssh/slurm-lab}
OUT=${OUT:-$(cd "$(dirname "$0")/.." && pwd)/internal/slurm/testdata}
PVE_API=${PVE_API:-https://10.0.2.1:8006/api2/json}
PVE_NODE=${PVE_NODE:-rogue}
C2_VMID=${C2_VMID:-132}

on() { local host=$1; shift; ssh -n -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$host" "$@"; }
log() { printf '\n==> %s\n' "$*"; }

capture() {
  local dir="$OUT/$1"
  mkdir -p "$dir"
  on "$CTL" 'scontrol show node --json' > "$dir/nodes.json"
  on "$CTL" 'squeue --json' > "$dir/squeue.json"
  on "$CTL" 'sacct --json -a -S now-1days' > "$dir/sacct.json"
  on "$CTL" 'sacctmgr -n -P show event event=node start=now-1days format=NodeName,TimeStart,TimeEnd,State,Reason,User' > "$dir/events.txt"
  on "$CTL" 'date +%s' > "$dir/now.txt" # triage evaluates the fixture as of this instant
  on "$CTL" 'sinfo -N -o "%N %T %E"' | sed "s/^/    /"
  log "captured $1"
}

# Power a lab VM on through the Proxmox API with the pool-scoped Terraform token.
pve_start() {
  : "${PROXMOX_VE_API_TOKEN:?export PROXMOX_VE_API_TOKEN (terraform@pve!lab=...)}"
  curl -fsSk -X POST -H "Authorization: PVEAPIToken=$PROXMOX_VE_API_TOKEN"     "$PVE_API/nodes/$PVE_NODE/qemu/$1/status/start" >/dev/null
}

# Poll scontrol's state flags (e.g. DOWN+NOT_RESPONDING) against an ERE.
# Don't poll sinfo's %T: after a node is set DOWN it can keep showing mixed*
# until the dead node's job allocation is released.
wait_for_state() { # node, state ERE, timeout seconds
  local deadline=$((SECONDS + $3))
  until on "$CTL" "scontrol show node $1" | grep -oE 'State=[A-Z_+]+' | grep -qE "$2"; do
    ((SECONDS < deadline)) || { echo "timed out waiting for $1 to be $2" >&2; return 1; }
    sleep 10
  done
}

resume_all() {
  on "$CTL" 'sudo scancel --user=labadmin' || true
  for n in slurm-c1 slurm-c2; do
    on "$CTL" "sudo scontrol update nodename=$n state=resume" 2>/dev/null || true
  done
  sleep 5
}

scenario_drained() {
  log "scenario: drained (health-check style reason on an idle node)"
  on "$CTL" 'sudo scontrol update nodename=slurm-c2 state=drain reason="NHC: check_fs_free: /tmp 98% used (threshold 90%)"'
  sleep 3
  capture drained
  resume_all
}

scenario_draining() {
  log "scenario: draining (maintenance drain while a job is still running)"
  on "$CTL" 'sbatch -w slurm-c1 -J long-train --wrap "sleep 900" >/dev/null'
  sleep 5
  on "$CTL" 'sudo scontrol update nodename=slurm-c1 state=drain reason="maint: kernel update CHG-1042"'
  sleep 3
  capture draining
  resume_all
}

# A GPU falls off the bus. With real GPUs, slurmd's NVML autodetect then finds
# 3 devices instead of 4, and slurmctld marks the node invalid. We reproduce that by
# removing one device node AND rewriting this node's local gres.conf to list only
# the survivors. (Deleting the device alone is a different failure: slurmd
# refuses to start with "fatal: can't stat gres.conf file /dev/fakegpu3", and
# the controller sees a node that has stopped responding.)
scenario_gres_missing() {
  log "scenario: gres-missing (GPU lost; slurmd registers 3 of 4 configured)"
  on "$C1" 'sudo rm -f /dev/fakegpu3 && sudo cp /etc/slurm/gres.conf /etc/slurm/gres.conf.orig && sudo sed -i "/^NodeName=slurm-c1 /s/fakegpu\[0-3\]/fakegpu[0-2]/" /etc/slurm/gres.conf && sudo systemctl restart slurmd'
  sleep 20
  capture gres-missing
  on "$C1" 'sudo mv /etc/slurm/gres.conf.orig /etc/slurm/gres.conf && sudo systemd-tmpfiles --create /etc/tmpfiles.d/fakegpu.conf && sudo systemctl restart slurmd'
  sleep 15
  resume_all
}

# Two captures from one failure:
#  - slurmd-unresponsive: slurmd killed, host fine. The controller shows
#    MIXED+NOT_RESPONDING and the job keeps running (slurmstepd outlives slurmd).
#  - node-down: the whole host dies mid-job (forced power-off). After
#    SlurmdTimeout the controller marks it DOWN and the job fails or requeues.
scenario_slurmd_unresponsive() {
  log "scenario: slurmd-unresponsive (slurmd SIGKILLed mid-job, host still up)"
  on "$CTL" 'sbatch -w slurm-c2 -J etl-shard-7 --wrap "sleep 900" >/dev/null'
  sleep 10
  on "$C2" 'sudo systemctl kill -s KILL slurmd; sudo systemctl stop slurmd'
  wait_for_state slurm-c2 NOT_RESPONDING 300
  capture slurmd-unresponsive
  on "$C2" 'sudo systemctl start slurmd'
  sleep 15
  resume_all
}

scenario_node_down() {
  log "scenario: node-down (host powered off mid-job; wait for SlurmdTimeout)"
  on "$CTL" 'sbatch -w slurm-c2 -J etl-shard-9 --wrap "sleep 900" >/dev/null'
  sleep 10
  on "$C2" 'sudo systemctl poweroff --force --force' || true # the SSH session dies with the host
  wait_for_state slurm-c2 DOWN 900
  sleep 20 # let the job-failure accounting land in slurmdbd
  capture node-down
  pve_start "$C2_VMID"
  until on "$C2" true 2>/dev/null; do sleep 5; done
  wait_for_state slurm-c2 'IDLE|MIXED' 300 || true # ReturnToService=1 brings it back
  resume_all
}

scenarios=("$@")
[[ ${#scenarios[@]} -gt 0 ]] || scenarios=(drained draining gres-missing slurmd-unresponsive node-down)
for s in "${scenarios[@]}"; do
  "scenario_${s//-/_}"
done

log "final state"
on "$CTL" 'sinfo -N -o "%N %T %E"'
