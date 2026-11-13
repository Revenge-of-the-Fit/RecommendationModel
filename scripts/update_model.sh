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
"${compose[@]}" build model-update
slots=$(run_python src/deployment.py status --shell)
read -r active target <<< "$slots"
case "$active:$target" in blue:green|green:blue) ;; *) echo 'Invalid model slots' >&2; exit 1 ;; esac
service=api
if [ "$target" = green ]; then service=api-green; fi
"${compose[@]}" build "$service"
run_python src/prepare_recent_data.py "$@" --model-path "/app/models/slots/$active/recommender.npz" --output "$run_directory/data"
run_python src/train.py --data-dir "$run_directory/data" --model-path "$run_directory/candidate.npz"
run_python src/deployment.py stage --data-dir "$run_directory/data" --candidate "$run_directory/candidate.npz"

"${compose[@]}" up -d --no-deps --no-build --force-recreate "$service"
echo "Waiting up to 10 minutes for $target; $active continues serving."
if ! run_python src/deployment.py wait "$target"; then
  "${compose[@]}" logs --tail=20 "$service"
  echo 'Replacement did not become ready. Traffic still uses the previous model.' >&2
  exit 1
fi
run_python src/deployment.py switch "$target" --record "$run_directory/deployment.json"
printf '\nUpdate evidence: %s/deployment.json\n' "$run_directory"
echo 'After Kafka captures the follow-up period, save availability evidence using scripts/save_update_evidence.sh.'
