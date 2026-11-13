# Production experiment tracking verification — November 13, 2026

Issue #41's infrastructure was deployed and exercised with real production traffic
using revision [9a727bb](https://github.com/Revenge-of-the-Fit/RecommendationModel/commit/9a727bbcd60f3e1021f9a0a2fe796a6c5e8f2bf5).
The [Linux CI run](https://github.com/Revenge-of-the-Fit/RecommendationModel/actions/runs/43474751828)
passed the infrastructure tests and container build. Local verification passed 520
tests and 403 subtests, with one platform-specific skip. See the
[experiment design and operating instructions](../M3_EXPERIMENTS.md).

This was an infrastructure smoke test comparing the November 13 retrained EASE
model (blue/control) with the October 29 model (green/treatment). It did not test
the cheaper-license ranking policy and is not the final M3 business experiment.

## Saved plan and completion

All times are UTC. The [registered plan](m3-experiment-2026-11-13/plan.json) records
the salt, model identifiers, fee snapshot hash, and outcome definitions before
enrollment began.

| Event | Time |
| --- | --- |
| Gateway with experiment logging started | 17:51:39.261409 |
| Plan registered | 17:55:26.772910 |
| Enrollment began | 17:58:17 |
| Enrollment ended | 18:13:17 |
| Last possible 15-minute user follow-up ended | 18:28:17 |
| Snapshot exported | 18:50:30.842675 |
| Experiment finished without aborting | 18:53:31.878218 |

The [completion record](m3-experiment-2026-11-13/completion.json) identifies this
run as completed, with `aborted: false`. The finish command removes the experiment
configuration and returns traffic to blue/control.

## Observed results

The [full aggregate results](m3-experiment-2026-11-13/results.json) and
[generated results table](m3-experiment-2026-11-13/results.md) are copied from the
VM export.

| Measure | Control | Treatment |
| --- | ---: | ---: |
| Enrolled users | 4,002 | 3,973 |
| Users with qualifying watches | 3,975 | 3,937 |
| Distinct user/movie viewing units | 3,993 | 3,954 |
| Units with known fees | 3,083 (77.21%) | 3,050 (77.14%) |
| Mean known fee per covered unit, USD | 0.303770 | 0.309041 |
| Full spend per assigned user | Unknown | Unknown |
| Users with qualifying ratings | 15 | 12 |
| Mean rating among raters | 7.333333 | 7.250000 |

Treatment minus control spending among **users with complete observed fees** was
$0.003782 per user (95% CI −$0.003461 to $0.011026; p = 0.306080). This conditional
comparison used 3,095 control and 3,072 treatment users, including users with no
observed watches. The full-cohort spending effect is unknown: 1,814 of 7,947 viewing
units had movie IDs absent from the fee catalog. They were not priced at zero.

The mean-rating difference among raters was −0.083333 points (95% CI −1.088360 to
0.921694; p = 0.865776). Only 27 users supplied eligible ratings, so this is a wide,
conditional estimate, not evidence that the models have equal satisfaction.

Gateway records contain 15,911 distinct requests, all with status 200 and none
using the other arm as fallback. Actual served slots were blue for 7,959 requests
and green for 7,952. These counts include requests after enrollment closed while
follow-up continued; only first requests inside enrollment define the 7,975-user
analysis cohort. These are gateway observations, not course Kafka availability
measurements. The writer reported all 15,911 accepted records written, with zero
failures, rejections, duplicates or queued records at export.

Both Kafka partitions bracket enrollment and full follow-up. The snapshot preserves
1,687,847 watch observations and 9,493 rating observations across the configured
window, including events for users outside the cohort. Analysis selects outcomes
inside each enrolled user's own follow-up. Capture counters at export recorded
1,305 invalid outcome messages across the broader capture period; those counters
are not a count of missing outcomes for enrolled users. Coverage bounds cannot
prove all source events were valid.

## Reproduction and retained inputs

The [snapshot manifest](m3-experiment-2026-11-13/snapshot.json) contains SHA-256
hashes for the plan, frozen fees, requests and outcomes. All four checksums passed
after copying the snapshot to the PC. Re-analysis matched the VM result, comparing
every field with a tolerance of 1e-12 for floating-point values. Independent checks
confirmed unique request IDs, ordered unique partition/offset pairs, timestamps
inside the planned window, assignment consistency and cohort counts.

The complete snapshot remains on the shared model volume at
`/app/models/experiments/tracking-smoke-20261113/snapshot/`. On the VM:

```bash
cd /home/production
sudo bash scripts/experiment.sh analyze --snapshot /app/models/experiments/tracking-smoke-20261113/snapshot
```

The local copy is under `data/m3-experiments/tracking-smoke-20261113/snapshot/`.
From the repository root with dependencies installed:

```bash
python src/experiment.py analyze --snapshot data/m3-experiments/tracking-smoke-20261113/snapshot
```

The files committed here are aggregate report evidence, not the complete analysis
snapshot. User-level request/outcome files and the copied fee catalog remain in the
Git-ignored `data` directory and on the VM for reproduction.

## Handoff to the final experiment

The tracking and analysis infrastructure is verified. Before the separate final
experiment, prepare the cheaper-license variant with a served identifier that
includes its ranking settings, provide a broader validated fee snapshot, and choose
enrollment/follow-up windows long enough to obtain more ratings. The current fee
snapshot leaves approximately 23% of observed units unpriced. Register the final
plan with a new ID and keep both variants fixed through follow-up; include the
final run's cost and rating results and required screenshot in the M3 report.
