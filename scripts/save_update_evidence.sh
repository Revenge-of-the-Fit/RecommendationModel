#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --project-name "${COMPOSE_PROJECT_NAME:-recommendationmodel}" run --rm --no-deps -T \
  model-update src/update_evidence.py "$@"
