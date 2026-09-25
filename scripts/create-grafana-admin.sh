#!/usr/bin/env bash
# One-time: create monitoring/grafana-admin with a random admin password.
# The password is generated on the control plane and read from a process
# substitution, so it never appears in argv, a file, or this terminal.
# Read it later with:
#   kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d
set -euo pipefail

CP=${CP:-labadmin@10.0.5.133}
SSH_KEY=${SSH_KEY:?SSH key for the control plane}
NAMESPACE=${NAMESPACE:-monitoring}

# shellcheck disable=SC2016  # expanded on the control plane, not here
ssh -T -o BatchMode=yes -o LogLevel=ERROR -i "$SSH_KEY" "$CP" bash -s -- "$NAMESPACE" <<'REMOTE'
set -euo pipefail
ns=$1
if kubectl -n "$ns" get secret grafana-admin >/dev/null 2>&1; then
  echo "secret $ns/grafana-admin already exists; nothing to do"
  exit 0
fi
kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$ns" create secret generic grafana-admin \
  --from-literal=admin-user=admin \
  --from-file=admin-password=<(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24) >/dev/null
echo "created $ns/grafana-admin (24-char random password)"
REMOTE
