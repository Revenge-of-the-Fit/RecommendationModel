#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
project="${COMPOSE_PROJECT_NAME:-recommendationmodel}"
[[ "$project" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || { echo 'Invalid Compose project name' >&2; exit 1; }
exec 9>"/tmp/${project}-model-update.lock"
flock -n 9 || { echo 'Another model update or traffic switch is running' >&2; exit 1; }
docker compose --project-name "$project" run --rm --no-deps -T model-update src/experiment.py "$@"
