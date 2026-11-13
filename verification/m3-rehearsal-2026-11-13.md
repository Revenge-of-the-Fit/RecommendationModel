# M3 serving and update rehearsal — November 13, 2026

These observations come from commands run by `wblackwo` on the team VM and the
returned production verification records. This is an implementation rehearsal,
outside the planned submission's final 72 hours. It does not satisfy the two
submission-window updates. Course Kafka logs confirm 2,651 successful requests out
of 2,651 observed in the ten-minute window around this update.

Deployed implementation and training revision:
[`dd657753cf83d152670e203b2d7d18594bd9b209`](https://github.com/Revenge-of-the-Fit/RecommendationModel/commit/dd657753cf83d152670e203b2d7d18594bd9b209).
Both infrastructure tests and the service-image build passed in
[CI run 36](https://github.com/Revenge-of-the-Fit/RecommendationModel/actions/runs/43406615146).
See [the deployment design and commands](../M3_DEPLOYMENT.md).

## Observed results

All timestamps below are UTC. Probe latencies are measured inside the VM; they
exclude the course-to-VM network path.

| Check | Observed result |
| --- | --- |
| Initial public-port migration | Gateway returned HTTP 200 from green on port 8082 at 10:46:57; an external PC request also returned 200 from green at 10:49:27. The one-time handoff duration was not measured. |
| Two ready servers | Blue and green both loaded the original model. Ports 8083/8084 were bound to localhost; the gateway owned public 8082. Both request-log health checks passed at 10:59. |
| Training | 5,000 users, 2,000 movies, 9,082 interactions; EASE regularization 100 and minimum positive rating 6. Training took 1.12 seconds. Actual training observations span 07:04:30–10:34:03 on November 13. |
| Updated model activated | Green → blue at **11:18:06.275868**. The script prepared/recreated blue and retained the serving green container and gateway. The candidate probe took 4.235 ms; the routed probe took 7.601 ms. Both returned 20 recommendations with persisted request records matching the new hash. |
| Course-observed availability | 2,651/2,651 requests returned status 200 during 11:13:06.275868–11:23:06.275868. Both partition samples bracket this window; the saved report marks the evidence usable. |
| Public endpoint after update | HTTP 200 with `x-model-slot: blue` and the new model hash at 11:18:25. |
| Both versions served concurrently | A 50/50 user split ran from **11:22:15.480466** to **11:22:39.291751**. The routed green probe took 7.152 ms and the blue probe took 4.633 ms; both returned 20 recommendations and matched their respective model hashes in the request log. |
| Final routing | Primary blue, secondary share 0%; generation `4a2556b8e3c441e8a53572b90496e12e`. Green remained available as fallback. |

Model SHA-256 values:

- Original/green: `673e4103a6095c76b6ce0181636dd5bb95386f0661e2d35bee9b5a5c20ee6676`
- Updated/blue: `f5ec8deec7698169786ec327ce77aa83a7aa8afe8a990bf2f43baa6efbd58215`

## Saved evidence and lookup

The shared model volume retains the run at:

`/app/models/updates/20261113T111208Z-3885278/`

It contains `candidate.npz`, [artifact-manifest.json](m3-2026-11-13/artifact-manifest.json), the exact training export
under `data/` (including `dataset-manifest.json` and the passing `data-quality.json`),
and these verified activation records:

| Record | Routed request IDs |
| --- | --- |
| [deployment.json](m3-2026-11-13/deployment.json) | Blue: `99e1dd9574f244268007f8a5f129d64f` |
| [traffic-split.json](m3-2026-11-13/traffic-split.json) | Green: `57d3b5a7a75c4a53b1ac2062c65fde0f`; blue: `f677b356a80447418652f7f4ac6ac58f` |
| [traffic-restored.json](m3-2026-11-13/traffic-restored.json) | Blue: `f9f7c07554024944b339775ffa17475e` |

The linked records are copies from the VM. Model artifacts and training data remain
in the shared model volume at the paths recorded in the manifest.

Request records are in `/app/state/events/events.sqlite3`, table
`recommendation_requests`, keyed by `request_id`. Decode `record_json` with
`storage.payloads.load_json` because records may be compressed. The unchanged
serving-catalog fingerprint is separate from the new training snapshot; use the
archived manifest's `training` section to identify training inputs.

## Course Kafka availability evidence

The [availability summary](m3-2026-11-13/availability.json) was collected at
**12:46:46.749622 UTC** for the exact server
`cmu-online-mlip-vm02.andrew.cmu.edu:8082`. Its window spans five minutes before and
after the switch. Success means course Kafka status 200; all observed recommendation
requests, including failures and timeouts, belong in the denominator.

| Period | Successful requests | Failed requests | Observed availability |
| --- | ---: | ---: | ---: |
| Before switch | 1,286 / 1,286 | 0 | 100% |
| After switch | 1,365 / 1,365 | 0 | 100% |
| Entire ten-minute window | 2,651 / 2,651 | 0 | 100% |

Both partition samples extend before the window's start and after its end, with no
unattributable recommendations; `usable` is true. The copied
[request observations](m3-2026-11-13/availability-requests.jsonl.gz) were independently
checked for unique partition/offset pairs, the exact server and time window, and
counts matching the summary. These observations establish availability for this
window, not the full M3 grading period.

The Kafka consumer restarted at 10:34:10 after an `OperationalError`; its log did
not identify the SQLite cause. At 11:02, stored events reached only 10:34:04. The
consumer's storage limits matched the healthy APIs, and subsequent logs showed
advancing replay offsets. By 12:46, captured events were current and covered the
update window; the restart count was still two with the same 10:34 start time. No
offsets were reset to skip history. The earlier incomplete export was replaced by
the usable evidence linked above.

## Remaining submission requirements

Two distinct updates and their course-log evidence are still required during the
final 72 hours before the actual M3 submission. Follow the
[update and evidence procedure](../M3_DEPLOYMENT.md#required-production-evidence),
include those records in the M3 report, and check their times against the actual
submission timestamp. Keep issue #40 open until that work is complete.
