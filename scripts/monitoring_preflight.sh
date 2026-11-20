#!/usr/bin/env bash
# Refuse to start the Grafana alerting stack when its private settings are missing.
# Usage on the VM:  set -a; . ./.env; set +a; bash scripts/monitoring_preflight.sh
set -eu

missing=0
for name in SLACK_WEBHOOK_URL GRAFANA_ADMIN_PASSWORD GRAFANA_ROOT_URL; do
  if [ -z "${!name:-}" ]; then
    echo "monitoring preflight: $name is not set (add it to the private .env)" >&2
    missing=1
  fi
done
exit "$missing"
