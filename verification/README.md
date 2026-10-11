# Milestone 1 verification

Checked October 11, 2026 on `CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU` from the `mfouly` account.

## Results

| Check | Result |
|---|---|
| Course traffic, Oct 10 12:20–Oct 11 12:20 UTC | 368,008 successes / 368,435 requests |
| Confirmed personalized successes | 215,227 / 368,435 = 58.417% |
| Personalized paths | 205,942 EASE; 9,285 cached cold-start |
| Actual ranking variation | 133,104 personalized users; 46,287 different ordered rankings |
| Course latency p50 / p95 / maximum | 15 / 40 / 631 ms; 17 successes over 600 ms |
| External HTTP checks | 24/24 valid, 61–225 ms, including six concurrent callers |
| After API replacement | Five more external requests returned 200, maximum 138 ms |
| Sampled persistent state | Three request records, three saved profiles, two live profiles, three cache files, and model checksum unchanged |
| Focused tests | 40 passed, including pending profiles, dependency failures, live ranking changes, and recovery |


## How we counted

`scripts/verify_production.py` reads SQLite without changing data or Kafka offsets. It filters course records to our server and the stated UTC window. The database key `(source, topic, partition, offset)` prevents replay duplicates. Status 200 only counts when the result contains 1–20 unique catalog IDs. Status 0 and other failures stay in the denominator.

To confirm personalization, it matches the user, SHA-256 of ordered IDs, and timestamps within five seconds to exactly one API record. One API record cannot confirm two course records. It counts only EASE and cached cold-start matches. Fourteen successful course records lacked a unique API match and were excluded from the personalized count. Manual HTTP probes cannot create course successes.

The window includes three code versions, matching revisions `ebe6cfb`, `08b35b8`, and `7ef70ef`. Their full fingerprints and counts are in the JSON evidence. All use model SHA-256 `642573e7bae94cd2c7281298004b3d9a4735bee885c0dcc846f3f87ea11bc75a`. The running API identifies `7ef70ef`; its source fingerprint also matches current main. The model manifest's older revision describes when the model was provisioned.

Kafka capture was receiving current events. Committed offsets lag because buffered watches pin the safe restart position; a Kafka restart may replay older records. We did not change the production consumer group or its offsets. The saved counts cover captured events, and a small gap from the topic head does not prove complete capture.

## Rerun the traffic count

From the repository on the laptop, with CMU VPN connected, replace `--start` and `--end` with the actual intended window. Both timestamps must include a timezone and span no more than 24 hours:

```bash
ssh <andrew-id>@CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU \
  'docker exec -i recommendationmodel-api-1 python - --storage-path /app/state/events/events.sqlite3 --catalog /app/data/movies.csv.gz --server CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU --start 2026-10-10T12:20:00Z --end 2026-10-11T12:20:00Z' \
  < scripts/verify_production.py > verification/traffic-submission.json
```

## Deployment checks

Startup reused the existing images, private configuration, tunnel, and volumes. We then replaced only the API container. The new container reused the sampled state and resumed healthy serving; Kafka stayed running. Course logs recorded 167 connection failures between 12:27:36 and 12:28:23 UTC during startup, so replacement currently interrupts service for about 48 seconds. This was not a fresh VM installation or a zero-downtime deployment.

Dependency outages were simulated in focused local tests against the same API source. We did not disable production metadata or OpenAI access.

## Evidence

- [Course traffic](traffic-2026-10-11.json) and [code revision fingerprints](code-revisions-2026-10-11.json)
- [External requests and API record audit](http-2026-10-11.json), [requests after restart](http-after-restart-2026-10-11.json)
- [State before](persistence-before-2026-10-11.json) and [state after](persistence-after-2026-10-11.json)
- [Startup/replacement commands and failures](deployment-2026-10-11.json), [Kafka capture](kafka-capture-2026-10-11.json)
- [Focused test results](focused-tests-2026-10-11.txt)

Deployment verification is complete for this configured VM. Production behavior and sampled thresholds pass; the submission-relative traffic check remains pending. Meeting notes will be added by the team later.
