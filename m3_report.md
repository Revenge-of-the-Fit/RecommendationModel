# Milestone 3 — Monitoring and Continuous Experimentation

## Experimentation infrastructure

The existing [gateway](src/api/gateway.py) assigns normalized user IDs to stable
50/50 arms using a saved salted hash. [Tracking and analysis](M3_EXPERIMENTS.md)
freeze the plan and serving IDs, retain assigned-arm requests and pre-compaction
Kafka watches/ratings, and export a checksummed snapshot. Users, not requests or
viewing minutes, are the independent analysis units. Two-sided Welch tests give
treatment-minus-control effects, approximate 95% confidence intervals and p-values
for per-user spending and per-rater mean ratings, allowing unequal variances.
Users with no watches remain in spending denominators; absent ratings are not zero.
Missing fees make full spending unknown, and rater selection limits satisfaction
claims. Mean fee per watched user/movie unit is reported descriptively.

The [eight-hour protocol](M3_EXPERIMENTS.md#assignment-and-outcomes)
enrolls for seven hours and follows each user for one hour, then automatically
returns traffic to control. Preparation archives the same EASE weights for both
arms and broadens/fixes the fee snapshot before registration. Configurations remain
fixed through follow-up. The [launcher](src/license_experiment.py) verifies the
shared artifact, fee checksum, ranking settings and serving provenance.

## Experiment results

The [implemented alternative](src/recommender.py) subtracts **0.01 × validated
licensing fee in USD** from eligible personalized EASE scores before selecting
top 20, with unchanged weights, exclusions and cold-start/popularity policies.
Unknown fees receive the maximum validated snapshot fee as a conservative ranking
penalty only; they remain unknown for spending analysis. This coefficient trades
0.003 EASE score units for a $0.30 fee difference and does not guarantee unchanged
ratings.

In a [read-only replay of 500 training profiles](verification/m3-license-policy-replay-2026-11-19.json),
mean recommended-slate fees fell from $0.327431 to $0.290691 (11.2%); top-20
overlap was 87.86% and mean original EASE score retention was 99.52%. These are
ranking diagnostics, not watched-movie savings or observed ratings. The live
experiment has not run, so cost/rating effects, statistical confidence and the
results screenshot are not yet available.

## Monitoring

A [collector](src/monitoring.py) reads retained Kafka events and LLM attempts every five minutes. It publishes [Prometheus metrics](src/monitoring_api.py) for course request availability, subsequent-watch HitRate@20 with a one-hour follow-up, movie-license and Decisions costs, and drift against the M2 reference. Course availability counts failures and timeouts. License cost uses one fee per observed user/movie pair. LLM estimates count attempts once; missing usage or fees are shown as unknown. The gateway's readiness check is a separate signal.

In a [November 14 VM sample](verification/m3-monitoring-2026-11-14.json), all 3,931 requests attributed to our server succeeded. HitRate@20 was 1.61% (112 of 6,947 eligible requests). Known license fees totaled $1,375.95 for 77.9% of observed pairs, so the full cost is unknown. Forty-six recorded Decisions attempts cost an estimated $0.00275. No strong drift was flagged.


See the [operating instructions](M3_OPERATIONS.md#monitoring). The recurring service, dashboard, alert delivery and screenshot are pending.

## Provenance

The [API](src/api/routes.py) returns a request ID. Its request log records the model hash and whether the response used EASE, a cold-start profile, or a fallback. New serving IDs also include ranking settings, so two policies can share the same model file and still be distinct. A [serving manifest](src/provenance.py) links that ID to the model artifact, training Git commit, EASE settings, and the exact input files on the VM. [Model updates](src/deployment.py) preserve the candidate and its training snapshot when slots are replaced.

We [traced request `99e1dd9574f244268007f8a5f129d64f`](verification/m3-provenance-2026-11-14.json) from the November 13 production switch. It returned 20 EASE recommendations from artifact `f5ec8dee…`, trained by [commit `dd65775`](https://github.com/Revenge-of-the-Fit/RecommendationModel/commit/dd657753cf83d152670e203b2d7d18594bd9b209)
with regularization 100 and minimum rating 6. The exact snapshot is
`/app/models/updates/20261113T111208Z-3885278/data`; all three file hashes and the artifact hash matched on the VM. 

[Lookup and retrieval instructions](M3_OPERATIONS.md#provenance) show how to repeat it with another request ID. This past request predates the new serving IDs, so the model lineage is verified and the ranking settings were not logged for that request.
