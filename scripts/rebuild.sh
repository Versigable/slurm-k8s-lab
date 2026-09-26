#!/usr/bin/env bash
# Destroy the whole lab and rebuild it from this repo, then prove it works.
#
#   scripts/rebuild.sh --yes-destroy
#
# Runs on the control node (Linux/WSL) with terraform, the ansible venv and go
# on PATH. Order:
#   1. terraform destroy + apply        6 VMs from the Debian 13 template
#   2. refresh SSH host keys            the VMs have new ones
#   3. ansible site.yml                 classic Slurm, kubeadm, Cilium, Argo CD
#   4. one-time secret scripts          tokens and keys that never live in git
#   5. ansible gitops-bootstrap.yml     root app; waits for every app Synced/Healthy
#   6. build + ansible triage.yml       node-triage MCP server and its credentials
#   7. verify.yml + k8s-verify.yml      end-to-end checks
#
# Everything the lab needs comes from git plus four inputs outside it:
# the Proxmox API token, the GitLab token file, the lab SSH key and the
# slurmdbd password generated into ansible/.secrets/.
set -euo pipefail

[[ ${1:-} == --yes-destroy ]] || { echo "this destroys every lab VM; re-run with --yes-destroy" >&2; exit 2; }

REPO=$(cd "$(dirname "$0")/.." && pwd)
PVE_TOKEN_FILE=${PVE_TOKEN_FILE:-$HOME/.config/slurm-k8s-lab/pve-token}
GITLAB_TOKEN_FILE=${GITLAB_TOKEN_FILE:?path to the KEY=value file holding the GitLab token}
GITLAB_TOKEN_VAR=${GITLAB_TOKEN_VAR:-GITLAB_TOKEN_SLURMLAB}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/slurm-lab}
# Other known_hosts files to refresh (e.g. the Windows one Claude Code's MCP ssh uses).
EXTRA_KNOWN_HOSTS=${EXTRA_KNOWN_HOSTS:-}
LAB_IPS=(10.0.5.130 10.0.5.131 10.0.5.132 10.0.5.133 10.0.5.134 10.0.5.135)

export PROXMOX_VE_API_TOKEN
PROXMOX_VE_API_TOKEN=$(cat "$PVE_TOKEN_FILE")
export ANSIBLE_CONFIG=$REPO/ansible/ansible.cfg ANSIBLE_PRIVATE_KEY_FILE=$SSH_KEY ANSIBLE_NOCOLOR=1

declare -a TIMES=()
START=$SECONDS
step() { # name command...
  local name=$1 t0=$SECONDS
  shift
  printf '\n==> %s\n' "$name"
  "$@"
  TIMES+=("$(printf '%-34s %4ds' "$name" $((SECONDS - t0)))")
}

tf() { (cd "$REPO/terraform" && terraform "$@" -input=false -no-color); }
play() { (cd "$REPO/ansible" && ansible-playbook "$@" </dev/null); }

wait_ssh() {
  for ip in "${LAB_IPS[@]}"; do
    until ssh -n -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o ConnectTimeout=5 -o LogLevel=ERROR "labadmin@$ip" true 2>/dev/null; do sleep 5; done
  done
}

# Trust-on-first-use, immediately after creating the VMs on our own LAN: drop the
# old keys and record the new ones, so tools that check host keys keep working.
refresh_host_keys() {
  local files=("$HOME/.ssh/known_hosts")
  [[ -n $EXTRA_KNOWN_HOSTS ]] && files+=("$EXTRA_KNOWN_HOSTS")
  for f in "${files[@]}"; do
    touch "$f"
    for ip in "${LAB_IPS[@]}"; do ssh-keygen -R "$ip" -f "$f" >/dev/null 2>&1 || true; done
    ssh-keyscan -T 10 -t ed25519 "${LAB_IPS[@]}" 2>/dev/null >> "$f"
    echo "refreshed ${#LAB_IPS[@]} host keys in $f"
  done
}

secrets() {
  local env=(GITLAB_TOKEN_FILE="$GITLAB_TOKEN_FILE" GITLAB_TOKEN_VAR="$GITLAB_TOKEN_VAR" SSH_KEY="$SSH_KEY")
  env "${env[@]}" bash "$REPO/scripts/create-slurm-auth.sh"
  env "${env[@]}" bash "$REPO/scripts/create-grafana-admin.sh"
  env "${env[@]}" bash "$REPO/scripts/register-argocd-repo.sh"
  env "${env[@]}" bash "$REPO/scripts/register-runner.sh"
}

build_triage() {
  (cd "$REPO/triage" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
    -ldflags "-s -w -X main.version=$(git -C "$REPO" describe --always --dirty)" -o bin/node-triage ./cmd/node-triage)
}

step "terraform destroy"                tf destroy -auto-approve
step "terraform apply"                  tf apply -auto-approve
step "wait for SSH on 6 VMs"            wait_ssh
step "refresh SSH host keys"            refresh_host_keys
step "site.yml (Slurm, kubeadm, Argo)"  play site.yml
step "one-time secrets"                 secrets
step "gitops-bootstrap.yml"             play gitops-bootstrap.yml
step "build node-triage"                build_triage
step "triage.yml"                       play triage.yml
step "verify.yml (classic Slurm)"       play verify.yml
step "k8s-verify.yml"                   play k8s-verify.yml

printf '\n==> rebuilt and verified\n'
printf '  %s\n' "${TIMES[@]}"
printf '  %-34s %4ds\n' "total" $((SECONDS - START))
