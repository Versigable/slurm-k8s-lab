#!/usr/bin/env bash
# One-time: create a project runner for this repo in GitLab and store its
# authentication token as a Kubernetes Secret on the lab cluster.
#
# The token goes GitLab API -> pipe -> kubectl on the control plane. It is never
# echoed, written to a file, or put on a command line.
#
#   GITLAB_TOKEN_FILE=~/.claude/secrets/gitlab.env GITLAB_TOKEN_VAR=GITLAB_TOKEN_SLURMLAB \
#   SSH_KEY=~/.ssh/claude-agent scripts/register-runner.sh
#
# The GitLab token needs the create_runner scope and Maintainer on the project.
set -euo pipefail

GITLAB_API=${GITLAB_API:-https://gitlab.ninjaprivacy.org/api/v4}
PROJECT=${PROJECT:-agent-dev%2Fslurm-k8s-lab}
GITLAB_TOKEN_FILE=${GITLAB_TOKEN_FILE:?path to a KEY=value env file holding the GitLab token}
GITLAB_TOKEN_VAR=${GITLAB_TOKEN_VAR:?name of the variable in that file}
CP=${CP:-labadmin@10.0.5.133}
SSH_KEY=${SSH_KEY:?SSH key for the control plane}
NAMESPACE=${NAMESPACE:-gitlab-ci}
TAG=${TAG:-k8s-lab}

# Pick an interpreter that actually runs (on Windows, "python3" can be the Store stub).
PY=
for candidate in python3 python; do
  if "$candidate" -c '' >/dev/null 2>&1; then PY=$candidate; break; fi
done
[[ -n $PY ]] || { echo "no working python found" >&2; exit 1; }

auth() { printf 'PRIVATE-TOKEN: %s' "$(sed -n "s/^${GITLAB_TOKEN_VAR}=//p" "$GITLAB_TOKEN_FILE" | tr -d '\r')"; }
cp_ssh() { ssh -T -o BatchMode=yes -o LogLevel=ERROR -i "$SSH_KEY" "$CP" "$@"; }

if cp_ssh "kubectl -n $NAMESPACE get secret gitlab-runner-token" >/dev/null 2>&1; then
  echo "secret $NAMESPACE/gitlab-runner-token already exists; nothing to do"
  exit 0
fi

project_id=$(curl -fsS -H "$(auth)" "$GITLAB_API/projects/$PROJECT" | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["id"])')
cp_ssh "kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f - >/dev/null"

# Create the runner; only the token field leaves Python, straight into kubectl.
curl -fsS -X POST -H "$(auth)" "$GITLAB_API/user/runners" \
    --data-urlencode "runner_type=project_type" \
    --data-urlencode "project_id=$project_id" \
    --data-urlencode "description=slurm-k8s-lab (Kubernetes executor on the lab cluster)" \
    --data-urlencode "tag_list=$TAG" \
    --data-urlencode "run_untagged=false" \
  | "$PY" -c 'import json,sys; sys.stdout.write(json.load(sys.stdin)["token"])' \
  | cp_ssh "kubectl -n $NAMESPACE create secret generic gitlab-runner-token \
              --from-literal=runner-registration-token= --from-file=runner-token=/dev/stdin >/dev/null"

echo "runner created for project $project_id (tag: $TAG); token stored in $NAMESPACE/gitlab-runner-token"
