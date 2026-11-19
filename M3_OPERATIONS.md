# M3 monitoring and model provenance

## How the service works

We read watches, ratings, and recommendation outcomes from the course Kafka stream and store them in SQLite. Our update job trains EASE on a snapshot of recent events and saves the model and its training files. The public gateway forwards requests to one of two copies of the recommendation API, blue or green. Each copy loads its own model file, so we can prepare and check an update in one slot while the other serves traffic. See [model switching](M3_DEPLOYMENT.md) for the update commands.

## Provenance

We use the request ID to find the response in SQLite. The record shows which method served it and the model file it used. The new serving ID also identifies the catalog, code, training snapshot, and ranking settings. Each slot writes a manifest when it loads a model. The manifest links the serving ID to the model file, Git commit, training settings, and exact training data.

Once the provenance code is deployed, you can run this on the VM to repeat our [saved November 13 trace](verification/m3-provenance-2026-11-14.json).

```bash
cd /home/production
sudo docker compose --project-name recommendationmodel exec -T api \
  python src/provenance.py 99e1dd9574f244268007f8a5f129d64f
```

The command checks the model file and the three training files against their saved checksums. It prints the Git revision, training settings, and file paths. Use those values with [the training script](src/train.py) to rerun training on the same inputs. For a new trace, get the `X-Request-ID` from a recommendation response and check that the output matches its `X-Served-Version`. Our November 13 request predates that header.

## Monitoring

We use the existing M2 quality and drift checks. The collector reads SQLite about every five minutes and publishes request availability, how often users watch one of their top 20 recommendations within an hour, estimated license and LLM costs, and drift from the M2 baseline. Availability includes course-reported failures and timeouts. Missing fees or token usage can leave cost totals unknown.

To repeat the [one-time VM check](verification/m3-monitoring-2026-11-14.json) using today's data, run the collector from the updated API image.

```bash
cd /home/production
sudo docker compose --project-name recommendationmodel exec -T api \
  python src/monitoring.py --fetch-missing-fees
```

To run it continuously, start the monitoring service from the updated checkout on the VM.

```bash
sudo docker compose --project-name recommendationmodel --profile monitoring \
  up -d --build --no-deps monitoring
curl -fsS http://127.0.0.1:9091/report
curl -fsS http://127.0.0.1:9091/metrics
```

Once the first collection finishes, `/report` shows the latest summary and `/metrics` exposes it for Prometheus. Scrape `monitoring:9091` from the Compose network or `127.0.0.1:9091` from the VM.

## Dashboard and alert

Prometheus, a blackbox probe and Grafana run in the same `monitoring` profile. Prometheus scrapes `monitoring:9091/metrics` (the gauges above) and probes the public gateway's `/health/ready` every 15 seconds. Grafana loads everything from [`monitoring/grafana/provisioning`](monitoring/grafana/provisioning): the datasource, the [M3 Production dashboard](monitoring/grafana/provisioning/dashboards/m3-production.json) (availability, quality, cost, drift, collector health), the Slack contact point and the alert rule. Nothing in the serving path depends on these containers.

**Required alert: Gateway unreachable.** It fires when `probe_success` for the gateway is below 1 for 2 minutes, or when no probe data arrives at all. It posts to the team Slack channel with an `@channel` mention (set with Grafana's `mentionChannel` option; every teammate is in the channel), naming the trigger, linking the [runbook](INCIDENT_RESPONSE.md) and the dashboard. A resolved message follows on recovery, and an unresolved alert repeats every 4 hours. Rule: [`rules.yml`](monitoring/grafana/provisioning/alerting/rules.yml); message: [`templates.yml`](monitoring/grafana/provisioning/alerting/templates.yml).

Put these in the VM's private `.env` (never in Git), then check and start:

```dotenv
SLACK_WEBHOOK_URL=<Slack incoming-webhook URL>
GRAFANA_ADMIN_PASSWORD=<strong password>
GRAFANA_ROOT_URL=http://<VM-address>:3000
```

```bash
cd /home/production
set -a; . ./.env; set +a
bash scripts/monitoring_preflight.sh
sudo docker compose --project-name recommendationmodel --profile monitoring \
  up -d --no-deps prometheus blackbox-exporter grafana monitoring
```

`--no-deps` leaves the gateway, APIs and Kafka consumer untouched. Check Prometheus targets (`http://127.0.0.1:9090/targets` through an SSH tunnel, or `docker compose exec prometheus wget -qO- localhost:9090/api/v1/targets`), open Grafana on port 3000 and log in as `admin`.

## Incident drill

The drill deliberately stops the public gateway, waits for the **Gateway unreachable** alert, restarts the gateway as soon as the alert has fired, then asks a teammate to confirm the Slack message arrived and checks recovery ([`scripts/incident_drill.py`](scripts/incident_drill.py), plan in [INCIDENT_RESPONSE.md](INCIDENT_RESPONSE.md)). Run it once, outside a model update, after announcing it in the team channel. The gateway is also restarted if the drill fails, is interrupted or the SSH session drops, but run it inside `tmux` anyway. It refuses to start the outage unless Grafana answers and the alert is currently inactive.

```bash
cd /home/production
export DRILL_GRAFANA_PASSWORD=<Grafana password>
sudo --preserve-env=DRILL_GRAFANA_PASSWORD python3 scripts/incident_drill.py --confirm-stop-gateway
```

It writes `verification/m3-drill-<date>.md` with timestamped events: gateway stopped, alert firing, alert received by a named teammate, gateway started, readiness and recommendation checks passed, alert resolved. Add the Slack and Grafana screenshots and the `python src/monitoring.py` availability output for the same window next to it. This drill record is the only incident file meant for Git; the timelines, postmortems and logs of real incidents go in a Slack canvas and on the VM, as described in [INCIDENT_RESPONSE.md](INCIDENT_RESPONSE.md).

Grafana refuses to start without `GRAFANA_ADMIN_PASSWORD`. The password is only applied when Grafana first creates its database, so if the `grafana_data` volume was initialised earlier, set it explicitly: `sudo docker compose --project-name recommendationmodel exec grafana grafana cli admin reset-admin-password '<password>'`.
