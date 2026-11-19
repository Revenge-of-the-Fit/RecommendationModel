# Licensing-cost experiment

## Design

Both arms load the same frozen EASE weights and serving catalog. Control uses the
existing ranking. Treatment changes only personalized candidate ranking. The
original `license-v1` run and default preparation use coefficient 0.01:

```text
adjusted_score = EASE_score - 0.01 * licensing_fee_USD
```

[The ranking implementation](src/recommender.py) applies the penalty before
selecting top 20, preserving seen/unpopular exclusions and popularity/movie-ID
tie breaks. LLM cold-start and popularity-only recommendations are unchanged.
EASE scores are relative relevance scores, not predicted ratings or probabilities.

Fees must have unique `movie_id` values and a `license_cost` column. Missing,
negative and nonfinite fees are unknown. Unknowns receive the maximum validated
snapshot fee as a conservative ranking penalty only; they remain unknown for
spending analysis. The rule, coefficient and fee checksum are included in serving
provenance. Fees are loaded once at startup.

A [500-profile replay](verification/m3-license-policy-replay-2026-11-19.json)
reduced mean recommended-slate fees from $0.327431 to $0.290691 (11.2%), retaining
87.86% of top-20 items and 99.52% of mean original EASE score. This is a
training-profile diagnostic, not measured watched-movie savings or rating quality.

## Stronger follow-up policy

The completed [v1 results](verification/m3-license-v1-results.json) found a 1.30%
reduction in **known-fee watched units**, not the 11.2% recommended-slate replay
reduction. Full spending remained unknown because roughly 12.6% of watched units
had no validated fee. Mean ratings among raters changed by +0.0814 points
(95% CI -0.0230 to +0.1859).

A fixed [2,000-user-per-arm diagnostic](verification/m3-license-v2-diagnostics.json)
found actual first-request treatment slates
6.30% cheaper and first-ranked movies 3.56% cheaper. Only 58.61% of control and
56.52% of treatment watched units occurred in that first slate. These are
exploratory attribution diagnostics, not a replacement outcome definition or proof
that a larger penalty achieves proportional savings.

The proposed **new** `license-v2` policy uses coefficient **0.06**, retaining the
same weights, candidate eligibility and fallback behavior. A 500-training-profile
comparison gave:

| Penalty per USD | Recommended-slate fee reduction | Original EASE score retained |
| ---: | ---: | ---: |
| 0.01 | 11.22% | 99.52% |
| 0.02 | 19.03% | 97.73% |
| 0.04 | 35.27% | 90.30% |
| 0.06 | 53.11% | 76.81% |
| 0.10 | 77.24% | 48.92% |

These are not predicted ratings or watched-cost effects. Coefficient 0.06 leaves
room for recommendation-to-watch dilution without the much larger relevance
displacement of 0.10. A **10% minimum / 20% desired** reduction is a hypothesis,
not a guaranteed outcome. The fixed rating-loss margin is **0.5 points**: the lower
endpoint of the rating-difference 95% CI must exceed -0.5 to support the guardrail
among observed raters. An interval crossing -0.5 is inconclusive, not a pass.
Use the full per-assigned-user spending effect for the spending target, and report
the watched-movie fee separately. Unknown fees prevent establishing the full
spending target; do not substitute a favorable conditional estimate.

The launcher now accepts `prepare --penalty` and `--rating-decline-margin`,
freezes them and the decision criteria in the specification, and checks runtime
settings against that saved specification. New specifications have a checksum;
existing v1 inputs remain valid and unchanged.

After exporting and finishing v1, set these values **before** running the
deployment procedure below. They prepare a new run, refreshing the expanded v1
fee snapshot instead of starting from the original narrow catalog:

```bash
export LICENSE_EXPERIMENT_ID=license-v2
export LICENSING_TREATMENT_PENALTY=0.06
export LICENSE_FEES_SOURCE=/app/models/license-policies/license-v1/fees.csv
```

Inspect the new fee-coverage diagnostics before proceeding; one bounded refresh
does not guarantee complete future-watch coverage. The temporary unpenalized-green
rollout step still uses `LICENSING_TREATMENT_PENALTY=0`; the final treatment
recreation must use **0.06**, not the overlay's backward-compatible 0.01 default.
Allow up to 30 minutes per backend for the shared profile-cache import.
Run `start --id license-v2` only after a separately approved readiness check.
Do not overwrite, re-label or pool v1 results with the stronger policy's results.

## Assignment and outcomes

- [The gateway](src/api/gateway.py) hashes a saved salt and normalized user ID for
  stable 50/50 user assignment.
- Enrollment lasts seven hours. A user's first request starts a fixed one-hour
  follow-up; repeated requests do not reset it. Failed requests and fallback stay
  in the assigned arm.
- The split continues through the last follow-up and automatically returns to
  blue/control at start + eight hours. Start is scheduled three minutes after
  registration.
- [The plan](src/experiment.py) freezes serving IDs, descriptions, assignment salt,
  observation windows, partitions and the copied fee snapshot.
- Gateway request logs and [pre-compaction Kafka capture](src/experiments/capture.py)
  preserve assignments, actual served versions, watches and ratings. Capture must
  succeed before Kafka offsets are acknowledged.

[Analysis](src/experiments/analysis.py) uses these outcomes:

| Measure | Definition |
| --- | --- |
| Spending per assigned user | Sum fees for distinct user/movie pairs watched strictly after first assignment and before the user's follow-up ends. Include users with no watches as zero. |
| Mean watched-movie fee | Total fees divided by distinct watched user/movie pairs; reported descriptively. |
| Mean rating per rater | Latest valid 1-10 rating per watched movie, submitted at/after its first qualifying watch and within follow-up; average within user, then equally across raters. |

Two-sided Welch tests report treatment-minus-control spending and rating
differences, 95% confidence intervals and p-values, treating users as independent
units and allowing unequal variances. Insufficient samples or zero variation
produce no claimed confidence. Intervals are approximate and per metric.

Unknown fees make full spending and its effect unknown; known spending and
complete-fee-user comparisons are labeled conditional. Missing ratings are not
zero, and rating results describe raters rather than every assigned user. Report
coverage and sample sizes alongside estimates. Do not stop or extend a run based
on significance. Spending is a viewing-unit proxy, not actual billing.

## Deploy and prepare

Run on the production VM with sudo access. Start with blue serving all traffic, both backends healthy,
and no registered experiment. Coordinate with other operators and pause scheduled
model updates during rollout. Preserve the existing `.env`, Kafka configuration,
project name and volumes. Do not recreate gateway/Kafka or reset offsets.

Run deployment and start from the same shell so the revision and experiment ID
remain set. Copy the committed source and build the three replacement images:

```bash
set -euo pipefail
sudo -v
cd /home/durbonas/RecommendationModel
revision=$(git rev-parse HEAD)
git archive HEAD | sudo tar -x --no-same-owner -C /home/production
cd /home/production
export APP_REVISION="$revision"
export LICENSE_EXPERIMENT_ID="${LICENSE_EXPERIMENT_ID:-license-v1}"
export LICENSING_TREATMENT_PENALTY="${LICENSING_TREATMENT_PENALTY:-0.01}"
export LICENSE_FEES_SOURCE="${LICENSE_FEES_SOURCE:-/app/data/movies.csv.gz}"

dc() {
  sudo env APP_REVISION="$APP_REVISION" LICENSE_EXPERIMENT_ID="$LICENSE_EXPERIMENT_ID" \
    LICENSING_TREATMENT_PENALTY="${LICENSING_TREATMENT_PENALTY:-0.01}" \
    docker compose --project-name recommendationmodel "$@"
}
license() { sudo env APP_REVISION="$APP_REVISION" bash scripts/license_experiment.sh "$@"; }
switch() { sudo env APP_REVISION="$APP_REVISION" bash scripts/switch_model.sh "$@"; }
wait_slot() { dc run --rm --no-deps -T model-update src/deployment.py wait "$1" --timeout 1800; }

dc build model-update api api-green
license prepare --id "$LICENSE_EXPERIMENT_ID" --fees "$LICENSE_FEES_SOURCE" \
  --penalty "$LICENSING_TREATMENT_PENALTY" --rating-decline-margin 0.5
```

[Preparation](src/license_experiment.py) archives the active EASE artifact and
training manifest. It broadens catalog prices using movie IDs from an indexed
tail of up to 100,000 retained events per partition and rate-limited course movie
metadata requests, then freezes the fee snapshot. Inspect the printed unknown-fee
count; recent coverage cannot guarantee coverage of future watches. Rejected
batches are split to isolate unknown movie IDs. Individual HTTP 400/404 responses
remain unknown; server/transport failures stop preparation. Lookups prioritize
frequently observed movies and are capped at 256 requests, with rejected/deferred
counts reported explicitly. An existing validated snapshot can instead be supplied with
`--fees /app/models/PATH.csv --no-refresh`.

Assets are retained under `/app/models/license-policies/<id>/`. Use a new ID for
each experiment; existing assets and saved snapshots are not overwritten.
Preparation does not change live routing.

Roll through green as unpenalized control, replace blue, then load treatment into
inactive green. Staging refuses an active slot and preserves its prior files.
Stop if any command fails:

```bash
license stage --id "$LICENSE_EXPERIMENT_ID" --slot green
LICENSING_TREATMENT_PENALTY=0 dc \
  -f compose.yaml -f compose.license-experiment.yaml up -d --no-deps --no-build api-green
wait_slot green
switch green

license stage --id "$LICENSE_EXPERIMENT_ID" --slot blue
dc \
  -f compose.yaml -f compose.license-experiment.yaml up -d --no-deps --no-build api
wait_slot blue
switch blue

dc \
  -f compose.yaml -f compose.license-experiment.yaml up -d --no-deps --no-build api-green
wait_slot green

curl -fsS http://127.0.0.1:8083/health/model
curl -fsS http://127.0.0.1:8084/health/model
curl -fsS http://127.0.0.1:8083/health/storage
curl -fsS http://127.0.0.1:8084/health/storage
curl -fsS http://127.0.0.1:8082/health/experiment
curl -fsS http://127.0.0.1:8082/health/routing
```

Confirm equal model/catalog versions, distinct serving IDs, healthy storage and
logging, no provenance errors, blue with no penalty, and green with the selected
coefficient and prepared fee checksum. Routing must remain blue/0%, with no experiment.
At this point the treatment is ready but the experiment has not started.

## Start

```bash
cd /home/production
sudo env APP_REVISION="$APP_REVISION" bash scripts/license_experiment.sh start --id "$LICENSE_EXPERIMENT_ID"
```

The launcher rechecks frozen weights, catalogs, ranking settings, fee checksum,
provenance and capture health before registration. Save the printed plan and its
`observation_end`. Registered experiments block model updates and traffic switches.
Do not rebuild/restart either arm, change fees, retrain, or send manual
recommendation probes during observation. The shell can exit after registration.

## Export and finish

After `observation_end` plus at least one minute:

```bash
cd /home/production
export APP_REVISION="$(git -C /home/durbonas/RecommendationModel rev-parse HEAD)"
run_id="${LICENSE_EXPERIMENT_ID:-license-v1}"
sudo env APP_REVISION="$APP_REVISION" bash scripts/experiment.sh export --id "$run_id"
sudo docker cp "$(sudo docker compose --project-name recommendationmodel ps -q api)":"/app/models/experiments/$run_id/snapshot/results.html" "/tmp/$run_id-results.html"
sudo bash scripts/experiment.sh finish
```

Export requires healthy/drained gateway logging, unchanged serving versions, no
gateway restart since enrollment, and Kafka partitions bracketing the complete
window. If ingestion is behind, wait and retry. Bounds establish time coverage,
not proof that every source event was valid or every client received a response.

The snapshot contains the plan, fees, requests, outcomes, checksums and aggregate
`results.json`, `results.md` and `results.html`. Open the HTML and capture both
licensing-cost and rating tables, including confidence intervals. Keep user-level
observations and course data private. Report coverage, sample sizes, arm means,
differences and uncertainty; distinguish cheaper watched movies from fewer watches.

Re-analyze the saved snapshot:

```bash
sudo bash scripts/experiment.sh analyze --snapshot /app/models/experiments/license-v1/snapshot
```

For emergency recovery, abort explicitly:

```bash
sudo bash scripts/experiment.sh finish --abort-reason 'Describe the incident'
```

An aborted run is not a completed experiment result. Historical infrastructure
verification is recorded in [the November 13 smoke test](verification/m3-experiment-2026-11-13.md).
