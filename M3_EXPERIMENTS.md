# Experiment tracking and analysis (issue #41)

This adds tracking and statistical analysis to the two model servers from #40.
The cheaper ranking policy and the final licensing-cost experiment belong to
their own tickets. An infrastructure smoke test with the existing models does
not answer the final cost-versus-ratings question.

## Design and fixed protocol

Before a run, [experiment.py](src/experiment.py) records an immutable plan with
the experiment ID, randomly generated salt, control slot, both served versions,
descriptions, enrollment times, follow-up duration, Kafka partitions, and a copied,
hashed movie-fee file. The same plan is archived and installed atomically in the
routing configuration. A new experiment needs a new ID.

The [gateway](src/api/gateway.py) uses SHA-256 of `salt:normalized_user_id` for a
stable 50/50 control/treatment assignment. Repeated requests and leading-zero
forms of a user ID keep the same assignment. Only control receives traffic before
the start and after the final follow-up ends. During the run, fallback remains
available; its actual slot/version is recorded separately from the assigned arm.

Enrollment is `[start, enrollment_end)`. A user's first gateway request in that
window starts their follow-up, even if serving fails. Outcomes are strictly after
that request and strictly before `first_request + follow_up_seconds`. Later
requests do not reset the clock. Both versions and the allocation stay fixed until
`enrollment_end + follow_up_seconds`. Users arriving after enrollment closes can
receive recommendations but do not enter the analysis cohort. This is an
assignment-based comparison; failed requests, fallback and users with no observed
watches remain in their assigned arm.

Gateway records reuse the asynchronous [request writer](src/storage/requests.py)
in a separate `experiment_logs` volume, including experiment, user, timestamp,
assigned arm/slot, response status, actual served slot/version, routing generation
and backend request ID. That ID links to the existing backend request log.
`/health/experiment` exposes write failures/rejections and the gateway start time.
An export refuses unhealthy or undrained logging, a changed served version, or a
gateway restart since enrollment began; an interrupted run must be repeated.

The Kafka consumer passes original parsed messages to
[OutcomeCapture](src/experiments/capture.py) **before** `WatchBuffer` coalesces them
or the main event store replaces watches. It saves compact watch/rating rows keyed
by partition/offset in a separate SQLite database for the fixed observation window.
Replay is idempotent; conflicting duplicates fail. Kafka offsets are acknowledged
only after these observations and the normal live-store writes succeed. A capture
failure stops ingestion so it can replay, while the recommendation service keeps
running. Each outcome database is capped at 8 GiB and respects the configured disk
and WAL safety limits. Retain the run until evidence is copied; maintenance of the
main event database does not delete these observations.

## Measures and statistical interpretation

| Metric | Data | Operationalization |
| --- | --- | --- |
| Licensing spend per assigned user, USD | First assignment, watch events, frozen movie fees | One viewing unit per distinct user/movie watched in that user's follow-up, including movies outside the recommendation slate. Sum its fees and average across all assigned users, including zero for users with no observed watches. This is the M2 viewing-unit proxy, not a count of true plays. |
| Mean fee per observed viewing unit, USD | Same watch/fee join | Divide summed fees by the number of distinct user/movie units. Report its arm difference descriptively. |
| Mean user rating, 1–10 | Submitted ratings for watched movies | Use the latest valid rating per user/movie submitted at or after its first qualifying watch and within the same follow-up. Average those ratings per user, then give each rater equal weight. Report the pooled submitted-rating mean separately. |

The fee file must have unique `movie_id` values and the named fee column. Missing,
negative or non-finite fees are unknown, never free. Results report missing catalog,
missing fee and invalid fee counts, fee coverage, and the known spending component.
If any observed unit lacks a fee, the full-arm total and spend-per-assigned-user
effect are unknown. A secondary comparison of users with complete fees is labeled
conditional; it is not an estimate for every assigned user. Rating coverage is
reported both per assigned user and per viewing unit; non-raters are not assigned
zero ratings. Ratings describe users who chose to rate, so changes in who rates
limit claims about satisfaction for all users.

[Analysis](src/experiments/analysis.py) reports treatment minus control with
two-sided Welch tests and 95% confidence intervals on **per-user** spending and
**per-user** mean ratings. See [SciPy's test and interval definition](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ttest_ind.html).
Users are the randomization and independent analysis units; repeated requests,
minutes and ratings are not independent samples. Welch allows unequal arm
variances. Its intervals are approximate, relying on independent users and adequate
sample sizes; small samples and strongly skewed spending require cautious
interpretation. Insufficient samples or no observed variation produce no claimed
confidence. The intervals are per metric, not simultaneous guarantees. A binomial
assignment-balance check is diagnostic. Set duration before starting and do not
stop when a p-value becomes significant; a non-significant result is inconclusive.

## Deployment and operation

Deploy the reviewed source before a run. Build `gateway`, `kafka` and
`model-update`, then recreate only gateway and Kafka. The two API containers and
their models do not need replacement for this ticket. Recreating the gateway has
a brief serving gap; do this before the final grading window. Let Kafka catch up
and confirm `/health/experiment` is healthy before registering a run. Use the
existing Compose project name, `.env`, Kafka group and volumes. Do not reset offsets.

All commands below run from `/home/production` on the VM. Replace the example
timestamps, descriptions and fee path with the values chosen **before** the real
experiment. Keep two distinct served version identifiers; if the ranking variants
share weights, their served identifiers must also distinguish ranking settings.
The ranking-policy ticket must provide those identifiers before the final run.

```bash
cd /home/production
curl -fsS http://127.0.0.1:8082/health/experiment
curl -fsS http://127.0.0.1:8082/health/routing
```

Register with a start at least two minutes ahead. This example enrolls for 12 hours
and follows each enrolled user for one hour; choose the actual windows before
running, allowing time to collect ratings. `--fees` defaults to
`/app/data/movies.csv.gz`, with column `license_cost`; a different validated fee
snapshot can be supplied with `--fees` and `--fee-column`.

```bash
sudo bash scripts/experiment.sh start --id license-v1 --start 'REPLACE_WITH_ISO_TIME_AND_TIMEZONE' --enrollment-hours 12 --follow-up-hours 1 --control-slot blue --control-description 'Current EASE' --treatment-description 'REPLACE_WITH_EXACT_FIXED_RANKING_POLICY'
```

The [wrapper](scripts/experiment.sh) shares the model-update/switch lock. Updates
and manual traffic switches refuse a registered experiment. Probe response headers
include `x-experiment-id` and `x-experiment-arm` during the active window. Do not use
manual probes during the final run; use a separate smoke-test ID and window for
those checks.

After the printed `observation_end` plus at least one minute, export and analyze:

```bash
sudo bash scripts/experiment.sh export --id license-v1
```

Export requires every declared Kafka partition to span enrollment through the
end of follow-up plus one minute of arrivals. If ingestion is behind, wait; do not
interpret a partial stream as zero watches. Invalid parsed outcome counts are
included as data-quality limitations. Bounds do not prove that every event was
valid or every response reached the client. Gateway telemetry describes responses
selected by the service, rather than course-confirmed receipt.

The default output is `/app/models/experiments/license-v1/snapshot/`, containing
`plan.json`, the fee snapshot, `requests.jsonl.gz`, `outcomes.jsonl.gz`, an input
checksum manifest, `results.json`, and a compact `results.md` table. The original
observations permit an exact re-analysis after live watch compaction or updates.
The exporter never overwrites a prior snapshot.

Reproduce the analysis without Kafka, API credentials or the live database:

```bash
sudo bash scripts/experiment.sh analyze --snapshot /app/models/experiments/license-v1/snapshot
```

Or, on a machine with project requirements installed, use
`python src/experiment.py analyze --snapshot PATH_TO_COPIED_SNAPSHOT`.
After the successful export, remove the experiment and return all traffic to its
control slot:

```bash
sudo bash scripts/experiment.sh finish
```

If recovery is needed during a run, explicitly abort and record why:

```bash
sudo bash scripts/experiment.sh finish --abort-reason 'Describe the incident requiring recovery'
```

An aborted run cannot supply the final experimental result. Once analysis is
complete, include the fixed plan, coverage, user counts, both cost and rating
results, differences and uncertainty in the report. Keep the full snapshot on the
VM or copy it for reproduction; share aggregate results in GitHub. The separate
live-experiment ticket supplies the final run and its required screenshot.

## Verification

`tests/test_experiments.py` exercises assignment/fallback logging, fixed windows,
repeated requests and watches, late observations, equal user weights, missing fees,
conditional ratings, Welch interval direction, immutable plans and snapshot
re-analysis. `tests/test_watch_consumer.py` checks that intermediate watches survive
coalescing and that failed capture cannot acknowledge Kafka offsets. Existing
gateway/deployment and online-evaluation tests remain in CI. All tests use local
fixtures; production deployment and smoke-test evidence must be recorded separately.
