# Pipeline implementation and testing: report contribution (issue #25)

This section is written to be pasted into the "Pipeline implementation and testing" part of `m2_report.md`. Numbers come from the run described under [Reproducing](#reproducing).

## How testing is organized

The pipeline is a set of independently runnable steps (`src/`), each with an entry point or a pure function that tests call directly. Production boundaries (Kafka, the user and movie APIs, the LLM provider) are injected or patched, so the suite needs no credentials, no network and no course data. This is enforced rather than assumed: `pytest.ini` disables sockets (loopback and unix sockets only), and a test that tries to reach an external host fails with `SocketConnectBlockedError`.

| Pipeline step | Code | Tests (count) | What they check |
|---|---|---|---|
| Collection / parsing of the Kafka stream | `events/parser.py`, `events/consumer.py`, `events/watch_buffer.py`, `consume_events.py` | `test_event_parser` (17), `test_event_consumer` (18), `test_watch_buffer` (10), `test_watch_consumer` (6), `test_consume_events` (11) | Parsing of each event type, malformed lines, offsets and commit behavior, restart and replay; consumer is driven with fake messages |
| Collection of user/movie metadata | `services/metadata*.py`, `collect_metadata.py` | `test_metadata_http` (21), `test_metadata_collection` (14), `test_collect_metadata` (10), `test_metadata_storage` (12) | HTTP failures, retries, rate/budget limits, redaction of credentials, caching; run against a local loopback server or stubs |
| Cleaning and features | `dataset.py`, `prepare_data.py` | **`test_dataset` (17, new)**, **`test_pipeline_scripts` (14, new)** | Schema, ID, range and referential validation of the three course files; one interaction per user/movie, watch counts, latest rating wins; cold-start user selection |
| Training orchestration | `train.py`, `recommender.py` | `test_recommender` (3), **`test_recommender_validation` (12, new)**, **`test_pipeline_scripts`** | Hyperparameter and training-data validation, failed fits leave no usable model; CLI exit codes and no model file on bad input. (EASE math itself has one reference test, not more.) |
| Evaluation | `evaluation.py`, `evaluate.py` | `test_evaluation` (15, 2 new) | Split disjointness, relevance threshold fixed across candidates, selection rule; **new:** the full run with the final test split trains only on train+validation (no test pairs), writes the report, and exits cleanly on invalid split fractions |
| Serialization / loading | `recommender.py` (`save`/`load`) | `test_recommender`, `test_recommender_validation` | Round trip keeps hyperparameters and rankings; missing file and unsupported format version are rejected |
| Serving | `api/`, `models/`, `services/recommendation.py`, `recommend.py` | `test_serving` (5), `test_live_recommender` (9), `test_first_request` (8), `test_pipeline_scripts` | Response format, routing between EASE / LLM cold start / popularity, invalid IDs, fallbacks, first-request personalization |
| Cold start | `cold_start.py`, `preferences.py`, `storage/profiles.py`, `prepare_cold_start.py` | `test_cold_start` (20), `test_profile_provenance` (24), **`test_profile_store_validation` (14, new)**, `test_live_users` (8), `test_live_worker` (11), **`test_pipeline_scripts`** | Provider mocked (no paid calls); caching and no duplicate paid call after restart; audit trail refuses inconsistent records; batch precompute only calls the provider for users with descriptions |
| Telemetry and storage | `api/request_logging.py`, `storage/*`, `compact_storage.py`, `manage_storage.py`, `export_observations.py` | `test_api_logging` (15), `test_storage*` (31), `test_event_storage` (14), `test_watch_compaction` (8), `test_export_observations` (7), `test_container_storage` (12) | Request logging, durability, compaction integrity, capacity guards, deployment configuration |

Total: 382 tests (355 subtests) in 32 files.

## Gaps found and filled (this ticket)

Before this ticket the data-handling steps with no direct tests were `dataset.py` (66% covered), `train.py`, `prepare_data.py`, `prepare_cold_start.py` (0%) and the failure paths of `EaseRecommender` and `ProfileStore`. Added, without duplicating existing coverage:

- `tests/sample_data.py`: a tiny synthetic course-format dataset (6 users, 6 movies, 15 events) with a user who has only a below-threshold rating, a user with a self-description, and a user without. No production data.
- `tests/test_dataset.py`: loading and typing; interaction building; every validation rule has a rejecting case (missing file/columns, empty tables, duplicate or non-positive IDs, unknown event type, unknown user or movie, unparseable timestamp, ratings out of range / missing / non-numeric / on the wrong event type, movies on account events).
- `tests/test_pipeline_scripts.py`: `prepare_data`, `train`, `recommend`, `prepare_cold_start` run as the user would run them (argv, exit codes, stdout/stderr, files written), on the sample data with the LLM client replaced by a mock.
- `tests/test_recommender_validation.py`: bad hyperparameters, inconsistent training data, untrained model, non-positive requests, seen and never-liked movies never recommended, deterministic ranking, profile-based recommendations and popularity fallbacks, load failures.
- `tests/test_profile_store_validation.py`: the provenance store rejects naive timestamps, blank IDs, changed inputs, early completion, mutated retained responses, profiles for failed or non-matching attempts, wrong content versions, and conflicting duplicates; replays are idempotent.
- `tests/test_evaluation.py` (+2): the full evaluation run (final test split). I checked the leakage assertion by temporarily adding the test rows to the final fit; the test failed, then I reverted the change.
- `pytest.ini`, `.coveragerc`, `requirements-dev.txt`: one command to run tests with branch coverage and the no-network guard.

## Coverage

Branch coverage over `src/` and `scripts/`: **90% overall** (was 85%; 4,414 statements, 1,338 branches). Per-module numbers: `python -m pytest --cov` (HTML via `--cov-report=html`). Core modules are at 94-100%: `recommender.py` 100%, `dataset.py` 98%, `train.py`/`prepare_data.py`/`prepare_cold_start.py` 100%, `events/consumer.py` 98%, `events/parser.py` 93%, `api/*` 92-100%, `storage/operations.py` 93%, `storage/profiles.py` 99% (was 76%).

Material code that remains untested, and why:

| Code | Coverage | Reason |
|---|---|---|
| `scripts/provision.py`, `scripts/check_first_requests.py`, `scripts/recovery_example.py` | 0% | Operate on the course VM, SSH, or the live service; they cannot run without production access. Their logic is thin glue over tested library code. |
| `scripts/benchmark_decisions.py` | 0% | Makes paid LLM calls by design. |
| `src/download_data.py` | 0% | Downloads from GitHub; the parsing and validation that follow are tested through `dataset.py`. |
| `scripts/verify_production.py` (84%) | lines 180-200 | The live-VM `main()`; the counting logic is covered by `test_production_verification`. |
| `preferences.py` | 90% | Legacy-cache import mismatch paths and a few defensive guards. Low risk; the import path is exercised on the happy path. |
| `compact_storage.main`, `export_observations` CLI (81-82%) | small | Argument handling around tested functions. |
| 6 deployment-config tests | skipped locally | Need `docker compose`; they run where it is installed (GitHub-hosted runners include it). |

Coverage is a map of what is not exercised, not the goal. We did not chase 100%, test the EASE algorithm beyond the existing reference check, or write tests that restate the implementation.

## Why this is adequate

- **Every transformation that feeds training is checked at its boundary.** Raw files are validated before any model is fitted, and both a bad file and a bad hyperparameter fail with a clear error and no model file written (`test_train_*`). A bad record cannot silently change the model.
- **Evaluation cannot leak.** Tests assert disjoint splits, a fixed relevance threshold during selection, and that the final fit never contains test pairs.
- **The live path degrades instead of failing.** Unknown users, users without descriptions, provider failures and invalid IDs each have a test that asserts the response type and that no paid call is made when it should not be.
- **The risky business rules have tests that can fail.** Cost control (no duplicate paid profile after restart), auditability (inconsistent provenance is rejected), and durability (compaction/recovery) are the places where a silent bug would cost the most, and each has direct tests.
- **Tests are hermetic and fast.** The suite runs in about a minute with no credentials, and the socket guard makes a regression in that property visible immediately.

Known limits: tests use small synthetic data, so they do not show model quality or performance at production scale (that is the job of offline/online evaluation), and the operational scripts listed above are verified by running them against the VM, not by unit tests.

## Reproducing

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest --cov --cov-report=term-missing --cov-report=html   # htmlcov/index.html
```
