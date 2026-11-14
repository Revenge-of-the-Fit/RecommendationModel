# M3 event storage (issue #51)

The production database was about 146 GiB on November 14, with about 58 GiB free
on the VM. This change compresses retained Kafka records in place and provides an
opt-in rolling budget of approximately **100 GiB of unpinned Kafka history,
including index overhead**, plus protected data. It does not promise that the
entire SQLite file will become 100 GiB.

## Representation and preservation

[event_payloads.py](src/storage/event_payloads.py) stores the exact raw value and
exact parser JSON together in a versioned zlib payload. The existing JSON column
contains a format marker. Every other column, source fingerprint, composite key,
and index stays unchanged. Null and empty raw values remain distinguishable.
Readers accept legacy and compressed rows; logical exports decode the payload.
Conversion verifies a lossless round trip before committing. Invalid or oversized
(over 64 MiB decoded) payloads stop the operation rather than being discarded.

This retains the existing WITHOUT ROWID layout. The production sample showed
53.32% savings with joint compression in that layout; this was a small nonrandom
sample, not a forecast for the entire database. Changing the primary-key layout
would require a larger migration for relatively little extra sampled benefit.

Only `kafka_events` is converted or expired. The following remain intact:

- live users, their watch/rating history, prepared profiles, and processing cursors;
- metadata snapshots/fetches, LLM attempts and token usage, preference profiles,
  profile uses, and the separate disk cache;
- recommendation request records, including model/profile references;
- model artifacts, manifests, exact training snapshots, data-quality references,
  and saved update/availability evidence;
- experiment plans, captured watch/rating observations, snapshots, and results.

The existing watch coalescing behavior remains: this database was already a
retained-event store, not an archive of every original movie-minute event.
Deleting expired raw rows does not erase the user's live interaction history or
invalidate a cached LLM profile. Tests exercise cache reuse after expiration and
restart without another provider call.

## Budget, protections, and operational limits

[event_history.py](src/storage/event_history.py) maintains small per-day,
per-partition accounting tables in the same transaction as event writes,
conversion, replacement, and expiration. The size estimate charges SQLite record
payloads, secondary-index entries, full overflow pages, and a 50% allowance for
local page packing. It avoids repeatedly scanning a 146 GiB database. It is an
approximation, not a measurement of occupied pages for each table.

Once conversion is complete, maintenance can expire the oldest eligible UTC days
until estimated unpinned history is within the target. Each pass touches at most
100 rows and approximately 8 MiB of source payloads (one larger row can exceed
that byte bound). It yields between passes. It always keeps at least ten complete
recent UTC days plus the current day. A late event gets protection based on the
latest of its event, broker, and ingestion timestamps; uncertain dates are kept.

Explicit pins protect entire UTC days intersecting their intervals, including
the ending day. Automatic pins protect registered experiments and experiments
without exported snapshots, and successful model updates whose Kafka availability
evidence has not yet been saved as usable. Configuring the actual M3 submission
time pins its full -72/+96-hour grading window. Pin an evaluation window before
starting an evaluation that needs data older than the ten-day minimum. Snapshot
files themselves are never removed by this feature.

Expiration waits for the live-history processing cursor to pass each record.
Expired Kafka offsets are remembered so replay does not refill expired history.
Retained offsets still receive the original fingerprint conflict check. A changed
record at an already-expired offset cannot be audited from this raw store.

**Protections take precedence over the target.** If ten days, pinned evidence, or
lagging processing prevents reaching 100 GiB, status reports that condition and
keeps the data. Other protected tables can still grow. Review the measured rate
after deployment; the remaining course duration is not a capacity guarantee.

No VACUUM, full database copy, replacement database, or large new index is needed.
Freed SQLite pages become reusable by all tables. `df` may stay unchanged while
`reusable_database_bytes` increases. Do not run VACUUM to make `df` look smaller:
this VM does not have space for a second full database. The existing standalone
watch compaction command is blocked after activation to prevent bypassing the
accounting tables.

Conversion/pruning yields below 16 GiB free or above a 256 MiB WAL. The existing
capacity checks and bounded checkpoints continue. Compose defaults now reserve
16 GiB and cap the database at 190 GiB; explicit VM `.env` settings still win.
Keep `STORAGE_MAX_BYTES` and `STORAGE_MAX_DATABASE_BYTES` consistent. Neither
setting should be reduced below the existing physical database file size.

## Staged deployment

Deploy while no experiment is registered. Use the normal source archive and
backup workflow in [M3_DEPLOYMENT.md](M3_DEPLOYMENT.md). Do not activate compression
until **both APIs, Kafka, model-update, and storage-maintenance** have the new
readers. Keep model artifacts, data, `.env`, and volumes intact.

1. Build those five service images. Leave the current primary serving.
2. Recreate the inactive API, wait for readiness, and verify its model using the
   existing deployment commands. Switch traffic to it and verify the public API.
3. Recreate the other API; wait and verify it. Restore the original primary.
4. Stop Kafka with a 120-second shutdown allowance, confirm exit code 0, then
   recreate it. Verify ingestion resumes and its offsets advance.
5. Recreate storage-maintenance. Its new read-only model mount supplies automatic
   evidence pins. The model-update image is used by the existing update scripts.
6. Inspect the inactive status and take a baseline before activation. Activation
   starts compressed new writes and resumable conversion; pruning stays disabled.

The gateway only routes requests and writes its separate experiment log; it does
not need a restart for this storage change. Do not rebuild unrelated images.

All commands below run on the VM in `/home/production`. Use these only after the
reader rollout above has been verified. Capture command output with the evidence.

```bash
cd /home/production
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py status
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py activate
sudo docker logs --tail 5 recommendationmodel-storage-maintenance-1
```

Review `converted_rows`, `payload_bytes_saved`, `reusable_database_bytes`, free
space, and WAL size as conversion proceeds. Conversion resumes from committed
partition cursors after a process restart. Avoid interpreting the size estimate
or counted days as complete until `conversion_complete` is true.

Before enabling deletion, inspect automatic pins, add any pending evaluation or
evidence windows, and record the planned/actual submission timestamp when known.
Replace the example dates/IDs below with the actual windows.

```bash
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py pin --id pending-evaluation --start '2026-11-01T00:00:00Z' --end '2026-11-04T00:00:00Z' --reason 'Evaluation inputs not yet exported'
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py configure --submission '2026-11-25T23:00:00Z'
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py status
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py enable-pruning
```

Update the submission timestamp if the actual submission changes. Release a
manual pin only after its evidence is saved and verified:

```bash
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py unpin --id pending-evaluation
```

To stop maintenance mutations, or disable only deletion:

```bash
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py pause
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py resume
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py disable-pruning
```

Pause leaves new event compression/accounting enabled, so ingestion continues
consistently. Keep compatible readers after activation; rolling back to old
images would make compressed rows unreadable. Expiration is irreversible for
unpinned raw records; protected state is not part of that operation.

## Production acceptance evidence

Before activation, create an evidence folder and capture a baseline:

```bash
mkdir -p /home/wblackwo/m3-storage-evidence
date -u | tee /home/wblackwo/m3-storage-evidence/baseline-time.txt
df -h /home/production | tee /home/wblackwo/m3-storage-evidence/baseline-disk.txt
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py status | tee /home/wblackwo/m3-storage-evidence/baseline-status.json
```

Keep the service operating for at least 24 hours after activation; also verify
conversion eventually completes. Capture the corresponding follow-up:

```bash
date -u | tee /home/wblackwo/m3-storage-evidence/followup-time.txt
df -h /home/production | tee /home/wblackwo/m3-storage-evidence/followup-disk.txt
sudo docker compose --project-name recommendationmodel exec -T storage-maintenance python src/manage_event_history.py status | tee /home/wblackwo/m3-storage-evidence/followup-status.json
sudo docker logs --since 24h recommendationmodel-storage-maintenance-1 > /home/wblackwo/m3-storage-evidence/maintenance-24h.log 2>&1
sudo docker logs --tail 20 --timestamps recommendationmodel-kafka-1 > /home/wblackwo/m3-storage-evidence/kafka-tail.log 2>&1
sudo docker compose --project-name recommendationmodel ps | tee /home/wblackwo/m3-storage-evidence/containers.txt
curl -fsS --max-time 3 -w '\n' http://127.0.0.1:8082/health/ready
curl -fsS --max-time 3 -w '\n' http://127.0.0.1:8082/health/storage
curl -fsS --max-time 3 -D - -w '\n' http://127.0.0.1:8082/recommend/1
```

Review the existing course-log availability evidence workflow as well; local
health probes alone do not establish course-observed availability. Compare the
baseline/follow-up database allocation, reusable pages, free space, Kafka estimate,
and protected windows. At 100 GiB the number of days is measured, not hard-coded.
If protected data or lag prevents meeting the target, keep those protections and
investigate the capacity report. Do not close #51 on local tests alone: rollout,
successful conversion, correct retention, and the 24-hour production check remain
required evidence.

Copy the evidence to the PC from PowerShell (repository root):

```powershell
New-Item -ItemType Directory -Force data/m3-storage-evidence | Out-Null
scp -r wblackwo@CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU:/home/wblackwo/m3-storage-evidence/. data/m3-storage-evidence/
```

## Verification

`python -m pytest` exercises the pipeline offline. The new tests cover byte-exact
round trips, corrupt payload rejection, interrupted conversion, live replacement
during conversion, resumed accounting, fingerprint conflicts, retention budget,
UTC minimums, pins, late arrivals, projection lag, expired replay, page reuse,
unchanged protected tables, LLM cache reuse, and worker shutdown. Existing model
update and Kafka availability tests run against both representations.
