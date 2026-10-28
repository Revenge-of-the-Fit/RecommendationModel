# Deployment

## Decisions cold start (issue #21)

The API uses OpenAI SDK 3.26.0 and `POST /v1/decisions` with `gpt-6-luna`.
Keep `OPENAI_API_KEY` in the existing private environment configuration.
Only genre questions relevant to the descriptions are sent; named-film/implicit
preferences use two genre-choice questions. Original description text drives the
local text ranker. No catalog or generated summaries are sent/required.

Profiles are cached by normalized descriptions, prompt, model, and interpretation
schema. Descriptions are whitespace-normalized and capped at 2,000 characters per
field. Existing prepared profiles remain usable while the new version refreshes.
Users with supported positive history and users without descriptions need no LLM call.

Signup events continue to queue preparation. An unprepared cold request wakes the
same single background worker and shares any existing job for that user. It waits
at most `COLD_START_WAIT_SECONDS` (default 0.4), within
`RECOMMENDATION_BUDGET_SECONDS` (default 0.5 from HTTP arrival). On timeout or failure
it returns the existing fallback; background work may finish for the next request.
`PROFILE_QUEUE_SIZE` defaults to 1024. Provider/metadata retries remain outside HTTP
waiting. Cached profiles and exclusions are reused immediately.

After uploading a reviewed revision, rebuild only the API with
`docker compose --project-name recommendationmodel up -d --build --no-deps api`.
This avoids rerunning fixed-M0 provisioning and preserves the named volumes.
Check readiness and run the two commands below inside the API container:

```bash
python scripts/benchmark_decisions.py --output /app/state/events/issue21-decisions.json
python scripts/check_first_requests.py --count 10 --output /app/state/events/issue21-first-requests.json
```

The first command makes 14 small paid calls using synthetic descriptions; the second
probes existing unprepared production users and reports first/later responses from
the request log. Its localhost timings exclude the course-to-VM network delay.
The benchmark reports token usage and latency; the HTTP check reports first/later
response timing, personalization, and fallbacks. Save generated reports outside Git.

Docker Compose provisions the verified catalog and model, then runs the API on port `8082`.
Named volumes preserve the model, catalog, cold-start cache, event logs, live profiles, and worker state.

## Start

The VM needs Docker Engine with the Compose plugin. Connect over SSH, then run:

```bash
ssh <user>@<VM-address>
git clone https://github.com/Revenge-of-the-Fit/RecommendationModel.git
cd RecommendationModel
```

Set up the course tunnel using the [course script](https://raw.githubusercontent.com/mlip-cmu-online/lab-kafka/main/connect-kafka):

```bash
curl -fsS https://raw.githubusercontent.com/mlip-cmu-online/lab-kafka/main/connect-kafka -o connect-kafka
bash ./connect-kafka kafkatunnel@128.2.24.239 9092
```

Use the tunnel and Kafka passwords from Canvas when prompted. The script starts a
background SSH tunnel on port `9092` and writes a private `~/.config/mlip-kafka.conf`.
The tunnel must be running on the VM that runs Docker.

Create a private `.env` in the checkout with the team OpenAI key and the **absolute**
configuration directory generated above:

```dotenv
OPENAI_API_KEY=<team key>
KAFKA_CONFIG_DIRECTORY=/home/<user>/.config
```

Then start:

```bash
chmod 600 .env
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
```

The Docker Kafka service can read the private configuration before dropping privileges.
Keep `.env` and `mlip-kafka.conf` outside Git. Port `8082` must be reachable by the course server.

The first start downloads the catalog and trains the model. Later starts reuse the model volume.
`RETRAIN_MODEL=1` rebuilds from the fixed M0 dataset. For recent production data,
use the update command below.

## Our existing VM

The shared deployment is already configured at `/home/production`. It is an exported
checkout, so it has no `.git` directory; its private `.env` supplies `APP_REVISION`.
From a teammate account, start the existing deployment with:

```bash
ssh <andrew-id>@CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU
cd /home/production
sudo docker compose --project-name recommendationmodel up -d --no-build --no-recreate
```

This reuses the existing images and state. To build a source update already copied into
this directory, use `up -d --build` and set `APP_REVISION` to that source's commit.
The existing tunnel and private configuration are shared team setup; a new login alone
does not replace them.

## Verify

```bash
docker compose ps
curl -fsS http://localhost:8082/health/ready
curl -fsS http://localhost:8082/recommend/1
docker compose exec api cat /app/models/artifact-manifest.json
docker compose exec api printenv APP_REVISION
```

The manifest revision describes model provisioning. `APP_REVISION` describes the API
deployment; the saved verification also checks the API source and model fingerprints.

Also test `http://<VM-address>:8082/recommend/1` from outside the VM. Allow inbound TCP
port `8082` in the VM firewall if required.

Example for our VM:
```bash
curl.exe -i http://CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU:8082/health/ready
curl.exe -i http://CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU:8082/recommend/1
```

To confirm profile state survives container replacement:

```bash
docker compose exec api sh -c 'echo ok > /app/state/profiles/persistence-check'
docker compose up -d --force-recreate api
docker compose exec api cat /app/state/profiles/persistence-check
```

On our configured VM, use `sudo docker compose --project-name recommendationmodel`
and add `--no-build --no-deps` to `up` when replacing only the existing API image.
Replacement currently interrupts serving while resources load (about 48 seconds in
the October 11 check). Real request/profile/cache checks are saved in [verification](verification/README.md).

## Update or stop

```bash
git pull --ff-only
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
docker compose down
```

`docker compose down` keeps the named volumes. Do not add `--volumes` unless the stored state
should be deleted.

## Storage format and recovery

Schema 7 stores large request-log and metadata-fetch JSON as tagged zlib BLOBs.
Small records remain TEXT. Readers support both forms; JSONL/evaluation exports
decode them back to the original JSON. Kafka events, timestamps, user/movie IDs,
ratings, metadata snapshots, profile provenance, LLM usage, and live summaries are
preserved. Rebuild API, Kafka, and storage-maintenance together for this change:
schema-6 code cannot read the new compressed records. SQL scripts that read these
two `record_json` columns must use `storage.payloads.load_json` instead of
`json.loads`. SQL JSON functions cannot operate directly on compressed BLOBs.

To convert the existing history, first build all three images while serving
continues. Then stop all three services. With them stopped, run the new API image:

```bash
docker compose --project-name recommendationmodel run --rm --no-deps --user 0 \
  --entrypoint python -T api src/compact_storage.py --offline \
  --output /app/state/events/events.compact.sqlite3
```

This builds a new densely packed database, verifies integrity, foreign keys and
every table's row count, and prints progress plus a final size report. It leaves
the original in place. It requires free space for twice the logical database
size plus 8 GiB, conservatively allowing index construction. Interruptions leave
the original usable. After successful verification, with writers still stopped,
checkpoint the original and retain it under a rollback filename before installing
the compact copy. Preserve its ownership. Never discard an uncheckpointed WAL.
Do not start schema-6 containers against the compact database.

Maintenance checks capacity/checkpoints every 60 seconds between backups.
Backup no longer runs
integrity scans against the live database or holds its source transaction while
validating/compressing the copy. Verification runs on the isolated copy; incremental
copying holds a fixed read snapshot so concurrent writes do not restart it, with
a ten-minute time budget. A failed backup preserves previous archives
and is retried later. Compose defaults to two verified backups within 24 GiB.
Verification and compression of a large isolated copy can take much longer than
the ten-minute live-copy budget. Inspect `storage_backup` log entries for the
result; temporary `.observations-*` files belong to the active backup. Retain the
pre-conversion database until a fresh backup has succeeded, then remove that
redundant copy to restore headroom for future backups.

Our VM's private `.env` sets `STORAGE_BUSY_TIMEOUT=5` for Kafka ingestion and
request-log writes. HTTP preparation waiting uses the separate budgets above.

Before write transactions, persistent writers check an 8 GiB free-space reserve
and a 4 GiB WAL limit. A blocked reader can therefore pause ingestion/logging
before exhausting the disk; serving existing recommendations can continue.
Maintenance can checkpoint the WAL after the reader closes. `STORAGE_MAX_BYTES`
still limits main-database pages (96 GiB in Compose). These guards and compression
do not provide indefinite history storage: they make growth smaller and bounded
failures visible. No training or evaluation history is automatically expired.

The October 25 deployment checks captured 457/457 successful course responses
within 600 ms. A fresh schema-7 backup passed integrity and compression checks;
removing the redundant original database left 145 GiB free (38% disk usage).

## Retrain on recent production data

With the current source deployed in `/home/production`, run:

```bash
sudo env APP_REVISION=<deployed-source-commit> bash scripts/update_model.sh
```

The [script](scripts/update_model.sh) builds the update/API images while the API
continues serving, prepares data, runs `MovieDataset` validation, invokes the
existing [trainer](src/train.py), validates and installs the candidate, then
recreates only the API. Kafka and maintenance stay running. Allow up to ten
minutes for API startup, including the existing profile-cache checks. A short
serving interruption is accepted for M2. No credentials are needed by training.

[Preparation](src/prepare_recent_data.py) defaults to `[now - 24 hours, now)` in
UTC. It reads at most 200,000 retained events, split equally between the captured
`movielog2` partitions, using their indexed offset tails. It keeps parsed watch
and rating events in the window that were ingested by its end, drops movies
outside the candidate catalog and invalid IDs/ratings, and selects at most 5,000
users by usable-event count (ties: user ID). The movie pool is fixed before reading
recent outcomes: up to 2,000 catalog movies ranked by the previous model's
popularity, with movie-ID ties. These caps bound EASE's dense matrices; this is an
activity-biased sample, not the complete day's data. Watch retention stores the
latest position per user/movie, so watch counts are not exact views or minutes.

Movie features and licensing costs reuse the existing provisioned catalog;
movies absent from it are excluded. User descriptions use the latest cached API
snapshot available by the cutoff, or blank strings if none exists. There are no
live metadata/API/LLM calls during preparation or training. The SQLite snapshot
is closed before file export/training, and reads have a 90-second SQL budget to
avoid holding the live journal open indefinitely. The window and limits can be
changed with `--hours`, `--end` (offset-aware ISO timestamp), `--max-events`,
`--max-users`, and `--max-movies` after the script name.

Each run is retained in the model volume at `/app/models/updates/<UTC-time>-<pid>`:

| File under `data/` | Format and purpose |
| --- | --- |
| `events.csv.gz`, `users.csv.gz`, `movies.csv.gz` | Existing `MovieDataset` format, ready for the trainer and offline evaluation. |
| `catalog.csv.gz` | Full provisioned movie metadata, including `license_cost` and runtime. |
| `kafka_events.jsonl.gz` | Window-filtered source sample before training user/movie filtering; source/topic/partition/offset, timestamps, IDs, parse status and `parsed` fields. Includes recommendation records for online evaluation. |
| `llm_usage.jsonl.gz` | At most 5,000 most recently inserted attempts filtered by start time; attempt ID, start/finish, status, model, endpoint and usage. Failed calls with recorded usage are included. Full prompts are omitted. |
| `dataset-manifest.json` | Window, actual training dates, limits, counts, offset bounds, cap indicators, previous model hash and export checksums. |

Evaluation owners should use the manifest to report sample coverage, select a
time split before fitting an evaluation model, and avoid treating retained watch
rows as exact paid views. The usage export reflects attempts' current recorded
state, not a historical reconstruction; missing usage must not be counted as
zero cost. Exports are read-only snapshots; no consumer-group offsets are changed.
Copy only the needed run's `data/` directory for evaluation, keeping production
user descriptions out of Git.

The candidate is saved as `candidate.npz` in its run directory. The
[installer](src/model_update.py) checks catalogs, array shapes, finite weights,
fallback recommendations, unchanged hyperparameters and export checksums before
replacing `/app/models/recommender.npz`. Failed preparation, training or validation
leaves that file untouched. An update lock prevents concurrent script runs, and
the previous artifact is retained as `/app/models/recommender.previous.npz`.
The existing manifest records training dates, export location and before/after
fingerprints. The final check requests a recommendation and matches its request
log's loaded model fingerprint to the candidate. Save that final JSON for the
report; a file replacement alone does not demonstrate a successful deployment.
If startup fails after installation, restore `recommender.previous.npz` and
recreate the API; the candidate/export remain available for inspection. Run
directories are not automatically expired; retain the run used for the M2 results
and remove unneeded older run directories when they are no longer in use.

### M2 report contribution

**Automated model updates:** A manually triggered [shell script](scripts/update_model.sh)
exports a bounded recent production sample with [preparation](src/prepare_recent_data.py),
validates it with `MovieDataset`, and runs the existing EASE [trainer](src/train.py)
with unchanged hyperparameters. It saves and [validates a candidate](src/model_update.py)
before replacing the model in the persistent volume and restarting the API through
Compose. Preparation/training/validation failures preserve the current artifact;
one previous artifact is retained. The deployment check verifies a successful
response was served with the candidate's fingerprint. Add the actual VM run's
training dates, old/new fingerprints and response result here after deployment.

**Pipeline structure:** [Kafka ingestion](src/consume_events.py) and the
[metadata client](src/services/metadata.py) persist observations in SQLite. Recent
data preparation exports the existing three-table dataset format; the
[dataset module](src/dataset.py) validates it and combines watches/ratings into
user/movie interactions. [EASE](src/recommender.py) fits and serializes weights,
and the update installer promotes the candidate. The
[recommendation service](src/services/recommendation.py) loads the model and uses
live history or the LLM cold-start path; [request logging](src/api/request_logging.py)
records outputs and model versions for evaluation. Evaluation is separate from
production fitting. The [update tests](tests/test_model_update.py) exercise sample
boundaries, metadata cutoffs, training compatibility, failed promotion, loaded
version verification and the shell script's failure behavior without production
connections.
