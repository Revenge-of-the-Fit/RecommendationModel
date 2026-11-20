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

Both runs used one-hour follow-up: seven-hour enrollment for v1 and three-hour
enrollment for v2. The gateway automatically returns traffic to control after
eight/four hours. The [launcher](src/license_experiment.py) freezes shared weights,
fees, coefficient, serving IDs and duration before exposure.

## Experiment results

The [alternative](src/recommender.py) ranks eligible personalized EASE candidates by
`score - penalty * validated_fee_USD` before top 20, with identical weights and
unchanged exclusions/cold-start/popularity policies. Unknown fees use the maximum
validated fee for ranking only, never for accounting.

| Observed outcome | [v1: penalty 0.01, 8h](verification/m3-license-v1-results.json) | [v2: penalty 0.04, 4h](verification/m3-license-v2-results.json) |
| --- | --- | --- |
| Users, control/treatment | 67,859 / 67,759 | 54,488 / 54,258 |
| Mean known watched fee, USD | 0.269475 / 0.265983 (-1.30%) | 0.259171 / 0.248301 (-4.19%) |
| Fee coverage | 87.44% / 87.33% | 92.04% / 91.73% |
| Raters, control/treatment | 1,599 / 1,687 | 1,618 / 1,593 |
| Mean rating | 7.0075 / 7.0889 | 7.2074 / 7.1751 |
| Rating difference [95% CI] | +0.0814 [-0.0230, +0.1859] | -0.0322 [-0.1393, +0.0748] |

Full spending savings are **unknown** due to missing fees. Conditional on users
with complete observed fees, spending/user changed by -$0.003370
(95% CI [-$0.005239, -$0.001502]; p=0.000408) and -$0.011273
([-$0.013408, -$0.009137]; p=4.55e-25). Neither rating change was conclusive
(p=0.1266/0.5552); v2 supports its -0.5-point rating guardrail among raters, not
the 10-20% full-spending goal. Separate periods/fee snapshots are not pooled.
See [deployment, measurement, samples and limitations](M3_EXPERIMENTS.md).

![Both live experiments: licensing costs and ratings](verification/m3-license-results.png)

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
