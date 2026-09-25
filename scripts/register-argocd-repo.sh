#!/usr/bin/env bash
# One-time: create a read-only GitLab deploy token for this repo and store it
# as an Argo CD repository Secret on the lab cluster.
#
# Python turns the API response into the Secret manifest and it's piped straight
# into `kubectl apply -f -`: the token is never echoed, written to disk, or put
# on any command line (not even kubectl's).
#
#   GITLAB_TOKEN_FILE=~/.claude/secrets/gitlab.env GITLAB_TOKEN_VAR=GITLAB_TOKEN_SLURMLAB \
#   SSH_KEY=~/.ssh/claude-agent scripts/register-argocd-repo.sh
#
# The GitLab token needs Maintainer on the project (deploy tokens) and api scope.
set -euo pipefail

GITLAB_API=${GITLAB_API:-https://gitlab.ninjaprivacy.org/api/v4}
PROJECT=${PROJECT:-agent-dev%2Fslurm-k8s-lab}
REPO_URL=${REPO_URL:-http://10.0.1.13:8081/agent-dev/slurm-k8s-lab.git}
GITLAB_TOKEN_FILE=${GITLAB_TOKEN_FILE:?path to a KEY=value env file holding the GitLab token}
GITLAB_TOKEN_VAR=${GITLAB_TOKEN_VAR:?name of the variable in that file}
CP=${CP:-labadmin@10.0.5.133}
SSH_KEY=${SSH_KEY:?SSH key for the control plane}
NAMESPACE=${NAMESPACE:-argocd}

# Pick an interpreter that actually runs (on Windows, "python3" can be the Store stub).
PY=
for candidate in python3 python; do
  if "$candidate" -c '' >/dev/null 2>&1; then PY=$candidate; break; fi
done
[[ -n $PY ]] || { echo "no working python found" >&2; exit 1; }

auth() { printf 'PRIVATE-TOKEN: %s' "$(sed -n "s/^${GITLAB_TOKEN_VAR}=//p" "$GITLAB_TOKEN_FILE" | tr -d '\r')"; }
cp_ssh() { ssh -T -o BatchMode=yes -o LogLevel=ERROR -i "$SSH_KEY" "$CP" "$@"; }

if [[ -n $(cp_ssh "kubectl -n $NAMESPACE get secret -l argocd.argoproj.io/secret-type=repository -o name") ]]; then
  echo "an Argo CD repository secret already exists in $NAMESPACE; nothing to do"
  exit 0
fi

curl -fsS -X POST -H "$(auth)" "$GITLAB_API/projects/$PROJECT/deploy_tokens" \
    --data-urlencode "name=argocd-slurm-k8s-lab" \
    --data-urlencode "scopes[]=read_repository" \
  | REPO_URL="$REPO_URL" NAMESPACE="$NAMESPACE" "$PY" -c '
import json, os, sys
t = json.load(sys.stdin)
secret = {
    "apiVersion": "v1", "kind": "Secret",
    "metadata": {"name": "slurm-k8s-lab-repo", "namespace": os.environ["NAMESPACE"],
                 "labels": {"argocd.argoproj.io/secret-type": "repository"}},
    "stringData": {"type": "git", "url": os.environ["REPO_URL"],
                   "username": t["username"], "password": t["token"]},
}
sys.stdout.write(json.dumps(secret))' \
  | cp_ssh "kubectl apply -f - >/dev/null"

echo "read-only deploy token created; stored as $NAMESPACE/slurm-k8s-lab-repo for $REPO_URL"
