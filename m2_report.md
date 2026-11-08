## Automated model updates

To start, we made it possible to retrain and deploy the model with one command, `sudo bash scripts/update_model.sh`, from `/home/production` on the VM. The [script](scripts/update_model.sh) gathers recent data through [preparation](src/prepare_recent_data.py), runs the data quality checks, and calls the existing [EASE trainer](src/train.py). The [installer](src/model_update.py) checks that the candidate loads before replacing the model and restarting the API. If preparation, training, or validation fails, the current model stays in place. We keep the previous model so that it can be restored if the new service fails to start. This approach accepts a short interruption while the API restarts.

On October 28, 2026, the script successfully trained and deployed a model using 13,619 production events from 02:32:40 to 05:09:30 UTC. These were a selected subset of the requested previous 24 hours. The model fingerprint changed from `6036093c...` to `47a2b22f...`. The deployment check then received HTTP 200 with 20 recommendations in 28.052 ms over localhost, and the saved request's model fingerprint matched the new artifact. The [deployment results](DEPLOYMENT.md#m2-report-contribution) include the full fingerprints and run details.

## Offline evaluation

We evaluated the model by hiding some recorded history and checking whether it recommended movies that the users had liked.

**Metric:** We used NDCG@10 because both finding a movie that somebody likes and placing it near the top matter. A hidden movie is relevant if its rating is at least 7 out of 10. Each relevant recommendation contributes `1 / log2(position + 1)`. We divide this sum by the best possible sum for that user's relevant movies, up to ten, then average across users. The score ranges from 0 to 1.

**Data:** We used the October 28 `movielog2` export, containing 13,619 events, 5,000 users, and 2,000 movies. The selected events span 02:32:40 to 05:09:30 UTC. We combined each user's watches and latest rating for a movie into one sample so that the same user/movie pair could not enter both training and testing.

**Operationalization:** We sorted samples by user/movie, then shuffled each user's samples using one NumPy `default_rng(42)`. For users with at least three samples, we reserved 20% for validation and another 20% for testing, rounding down but keeping at least one sample in each. Users with fewer than three samples stayed in training. Since regularization 100 and minimum positive rating 6 were already fixed, we combined training and validation and fit EASE once on 8,101 samples. The 152 test samples were not used to choose parameters.

We learned model weights, user profiles, and popularity from training data only. Recommendations excluded training-history movies. Ties used training popularity, then movie ID. Relevant test movies that the model could not recommend still counted toward the score. [Run instructions](OFFLINE_EVALUATION.md#run), [evaluation command](src/evaluate.py), [split and metric implementation](src/evaluation.py), and [data preparation](src/dataset.py).

On November 3, NDCG@10 was **0.170877 across 45 users**. Of the 5,000 users, 4,848 had too little history for a test sample, and another 107 had no relevant test rating. This leaves a small sample. In addition, the export favors active users and movies selected using the previous model's popularity. Users and movies intentionally repeat across splits because we are checking existing-user preferences. This random holdout does not measure future traffic or cold-start accuracy.

## Online evaluation

### Accuracy

**Metric:** We used subsequent-watch HitRate@20 to check whether recommendations matched what people later watched. A response is a hit if the same user watches any recommended movie within one hour. Responses without an observed watch count as misses. We also measured the hit rate among only responses with an observed watch.

**Data:** The October 29 export contains 200,000 retained Kafka events, including user/movie IDs, timestamps, recommendation lists, and watches. We evaluated successful responses from our server from **03:15 UTC inclusive to 05:00 UTC exclusive**, with observations through 06:00 UTC. The manifest identifies reference model `47a2b22f...`, but does not verify the model served for every request.

**Operationalization:** We matched exact user/movie IDs at strictly later times, before one hour elapsed. Duplicate source records and identical user/time/ranked-list responses counted once. Each response had its full follow-up inside both partitions' observed time spans. **7,553 of 34,357 responses hit, or 21.98%.** Among the **11,642 responses with observed watching**, the hit rate was **64.88%**.

### Runtime cost

**Licensing metric:** USD for observed user/movie pairs. These approximate views because the retained data does not identify viewing sessions. **Data:** Watches in the same window and saved catalog `license_cost` fees. **Operationalization:** We summed the fee once per distinct pair, including watches without a recommendation match. Minute chunks and repeated views of the same pair did not add fees. **41,966 of 51,578 pairs** had fees, totaling **USD 13,783.43**. The other **9,612** had unknown cost.

**LLM metric:** Token consumption and estimated USD. **Data:** The provider's 105 minute buckets for the same window recorded **1,793 Decisions requests**, **1,115,057 uncached input tokens**, and zero output or cache tokens for `gpt-6-luna` on the default tier. **Operationalization:** We summed the buckets once and calculated `1,115,057 / 1,000,000 * USD 0.10 = USD 0.1115057`. This uses [standard Decisions pricing](https://developers.openai.com/api/docs/guides/decisions#pricing-and-availability), checked November 8, 2026. Output tokens are not charged.

The [online evaluator](src/evaluate_online.py) deduplicates attempt IDs and counts distinct retries and failed attempts with usage once each. Reusing a prepared profile adds no provider call. Missing usage is unknown, not zero. We do not add application totals to provider totals. [Matching/cost tests](tests/test_online_evaluation.py); [run and manual pricing instructions](DEPLOYMENT.md#read-only-online-accuracy-and-cost-evaluation-issue-32).

The retained events omit history, so these results describe the saved sample. One watch can match multiple responses, and a later movie position can continue an earlier viewing. The accuracy measure does not establish causation, and the licensing subtotal misses unknown fees and separate repeated viewings.

## Data quality

We check schemas at the Kafka stream, user API, movie API, and recommendation API. Checks cover IDs, timestamps, ratings, metadata field types and ranges, and recommendation format. Supported missing metadata is allowed. Invalid Kafka or metadata records are audited and excluded, invalid requests return 422, and users without usable preferences follow the existing fallback.

Our [production checks](src/check_data_quality.py) compare a fixed reference with up to 5,000 Kafka records, 5,000 request logs, and 100 records from each metadata API. We compare schema validity, non-watch event types and ratings, user ages and description presence, movie genres/runtime/cost, and API status/method/latency. We exclude watch frequency because the database merges minute chunks. [Validation and drift implementation](src/data_quality.py).

Strong drift means total variation distance, half the sum of absolute category-proportion differences, exceeds 0.30. Sources need 50 observations; features below 50 are unassessed. Insufficient source data or more than 10% invalid records also fails the check. Preparation blocks updates that fail, preserving the deployed model.

On October 29, checks passed with no schema failures and a maximum drift distance of 0.10787. [Tests](tests/test_data_quality.py) cover valid and malformed inputs, strong shifts in all four sources, and insufficient data.

## Pipeline implementation and testing

We kept the pipeline in separate modules so that each step could be run and tested without having to run the entire service. [Kafka ingestion](src/consume_events.py) and the [metadata client](src/services/metadata.py) collect observations in SQLite. [Preparation](src/prepare_recent_data.py) exports recent data, and the [dataset module](src/dataset.py) validates it and combines watches and ratings into features. The [trainer](src/train.py) fits and saves [EASE](src/recommender.py), while the [installer](src/model_update.py) validates and promotes the artifact.

The [recommendation service](src/services/recommendation.py) loads the model and uses the user's available history. For users without supported history, the [background worker](src/services/live_worker.py) prepares and saves a Decisions preference profile from their likes and dislikes for cold-start ranking. Slow or failed preparation leaves a valid fallback available. [Request logging](src/api/request_logging.py) records recommendations, serving methods, timings, and model versions. Offline and online evaluation run separately from production training.

Our [test suite](tests/) uses small fixtures, temporary databases, and fake providers. Tests cover collection/parsing, cleaning and features, training orchestration, evaluation splits and scoring, model serialization/loading, serving, cold start, and telemetry. They include malformed inputs, duplicate/replayed events, cache reuse, different preferences, slow or failed providers, and failed model updates. The [network guard](tests/network_guard.py), installed by [conftest](tests/conftest.py), blocks external hosts and real Kafka clients. Tests do not need course credentials, production APIs, or paid LLM calls.

The [November 8 CI run](https://github.com/Revenge-of-the-Fit/RecommendationModel/actions/runs/42452393556) passed **481 tests with 89% overall line/branch coverage**. Its [coverage report](https://github.com/Revenge-of-the-Fit/RecommendationModel/actions/runs/42452393556/artifacts/13605832755) contains HTML/XML results by file.

These tests are appropriate because they check the transformations and connections between steps as well as failure handling. For example, held-out pairs cannot enter training, invalid records cannot silently become usable data, and failed candidate validation cannot replace the running model. The tests do not prove accuracy or latency at production scale, which require separate measurements.

## Continuous integration

We use [GitHub Actions](.github/workflows/ci.yml) to run the infrastructure tests on every push and pull request. The test job runs on Ubuntu with Python 3.12, installs [development dependencies](requirements-dev.txt), and runs pytest with line/branch coverage. It uploads JUnit results, test logs, and HTML/XML coverage reports even when tests fail.

In addition, a second job builds the service's Docker image and validates the Compose configuration. The workflow does not deploy to the VM or run production training. This allows us to check proposed changes without giving CI production access. Both jobs passed in the [November 8 run](https://github.com/Revenge-of-the-Fit/RecommendationModel/actions/runs/42452393556) for implementation commit [`2fa0202`](https://github.com/Revenge-of-the-Fit/RecommendationModel/commit/2fa02024daf23432a1c9a8571cf841aaa538cf50).

## Pull requests

We used branches and pull requests to divide the work. Another teammate reviewed the changes before merging, and CI checked the proposed implementation. Three examples with teammate approval are [data quality #35](https://github.com/Revenge-of-the-Fit/RecommendationModel/pull/35), [offline evaluation #37](https://github.com/Revenge-of-the-Fit/RecommendationModel/pull/37), and [online evaluation #38](https://github.com/Revenge-of-the-Fit/RecommendationModel/pull/38).

## Team contract update

no update to team contract
