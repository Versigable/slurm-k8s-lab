#!/usr/bin/env bash
# One-time: create the Slinky cluster's Slurm auth key and JWT key as Secrets.
# The chart can't safely generate them under Argo CD (lookup + random data on
# immutable Secrets). Keys are generated on the control plane from
# /dev/urandom via process substitution: never in argv, a file, or this terminal.
set -euo pipefail

CP=${CP:-labadmin@10.0.5.133}
SSH_KEY=${SSH_KEY:?SSH key for the control plane}
NAMESPACE=${NAMESPACE:-slurm}

# shellcheck disable=SC2016  # expanded on the control plane, not here
ssh -T -o BatchMode=yes -o LogLevel=ERROR -i "$SSH_KEY" "$CP" bash -s -- "$NAMESPACE" <<'REMOTE'
set -euo pipefail
ns=$1
kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
make_secret() { # name key
  if kubectl -n "$ns" get secret "$1" >/dev/null 2>&1; then
    echo "secret $ns/$1 already exists; keeping it"
    return
  fi
  kubectl -n "$ns" create secret generic "$1" --from-file="$2"=<(head -c 1024 /dev/urandom) >/dev/null
  echo "created $ns/$1 (1024 random bytes)"
}
make_secret slurm-auth-slurm slurm.key
make_secret slurm-auth-jwt jwt.key
REMOTE
