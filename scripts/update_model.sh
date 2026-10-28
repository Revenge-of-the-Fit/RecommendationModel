#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

project="${COMPOSE_PROJECT_NAME:-recommendationmodel}"
[[ "$project" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || { echo 'Invalid Compose project name' >&2; exit 1; }
exec 9>"/tmp/${project}-model-update.lock"
flock -n 9 || { echo 'Another model update is running' >&2; exit 1; }
compose=(docker compose --project-name "$project")
run_python() { "${compose[@]}" run --rm --no-deps -T model-update "$@"; }
run_directory="/app/models/updates/$(date -u +%Y%m%dT%H%M%SZ)-$$"

"${compose[@]}" config --quiet
"${compose[@]}" build model-update api
run_python src/prepare_recent_data.py --output "$run_directory/data" "$@"
run_python src/train.py --data-dir "$run_directory/data" --model-path "$run_directory/candidate.npz"
run_python src/model_update.py install --data-dir "$run_directory/data" --candidate "$run_directory/candidate.npz"

"${compose[@]}" up -d --no-deps --no-build --force-recreate api
ready=0
for attempt in $(seq 1 120); do
  if curl -fsS --max-time 2 http://127.0.0.1:8082/health/ready >/dev/null 2>&1; then
    ready=1
    break
  fi
  if [ $((attempt % 6)) -eq 0 ]; then
    printf 'Waiting for API startup (%s seconds)...\n' "$((attempt * 5))"
  fi
  sleep 5
done
if [ "$ready" -ne 1 ]; then
  "${compose[@]}" logs --tail=20 api
  echo 'Updated API did not become ready; previous model is saved as recommender.previous.npz.' >&2
  exit 1
fi
"${compose[@]}" exec -T api python src/model_update.py verify
