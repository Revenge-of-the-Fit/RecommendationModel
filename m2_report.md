## Offline evaluation

We checked the recommender by hiding some recorded movie history, training on the rest, and seeing whether it recommended movies the users had liked.

**Metric:** We used NDCG@10, a score from 0 to 1 for the top ten recommendations. Higher is better. A hidden movie counts as relevant when the user rated it at least 7. Finding it near the top earns more credit.

Each match adds `1 / log2(position + 1)` to the user's score. We divide by the best possible total for that user's relevant movies, up to ten, then average across users. Missed movies earn no credit.

**Data:** We used the saved October 28 `movielog2` export: 13,619 events, 5,000 users, and 2,000 movies. The selected events span 02:32:40–05:09:30 UTC on October 28.
This is a selected subset of the requested 24 hours. We checked that the copied files matched the saved export.

**Operationalization (how we ran it):** We combined each person's watches and latest rating for one movie into one sample. Keeping these records together prevents the same sample from entering both training and testing.

We reused the existing split: sort samples by user/movie, then shuffle each user's samples with one NumPy `default_rng(42)`. For users with at least three samples, reserve two groups of 20%, rounding down but keeping at least one in each. The second group is the test set. We combine the first group with the remaining training data because the model settings are already fixed. Users with fewer than three samples stay in training.

We trained a fresh EASE model once on 8,101 samples, keeping 152 samples for testing. Production settings stayed at regularization 100 and minimum positive rating 6. We learned weights, user profiles, and popularity only from training data. Recommendations exclude movies already in the user's training history. Ties use training popularity, then movie ID. Relevant test movies the model cannot recommend still count toward the score.

[Run instructions](OFFLINE_EVALUATION.md#run), [evaluation command](src/evaluate.py),
[split and scoring](src/evaluation.py), and [data preparation](src/dataset.py).

**Result:** On November 3, 2026, NDCG@10 was 0.170877 across 45 users.
Of the 5,000 users, 4,848 had too little history for a test sample. Of the remaining 152, another 107 had no hidden movie rated at least 7, leaving 45 we could score. Each had one highly rated hidden movie: it appeared in the top ten for 16 users, and was missing for 29. Its position determined how much NDCG credit it earned.

**Limits:** Only 45 users qualified, so this is a small sample. The export favors active users and movies selected using the previous model's popularity, capped at 200,000 source records, 5,000 users, and 2,000 movies. Users and movies repeat across the training and test groups. This check measures existing-user preferences; future traffic and the LLM cold-start path need separate evaluation.

## Online evaluation

### Accuracy

**Metrics:** Subsequent-watch HitRate@20 counts a response as a hit if the same
user has any recorded watch of a top-20 movie within one hour. Overall HitRate
includes responses without observed watches; conditional HitRate considers only
responses with any observed watch in that hour.

**Data:** The saved October 29 `movielog2` export contains 200,000 retained events.
We evaluated successful responses from our server during **03:15–05:00 UTC**,
with observations through **06:00 UTC**. The snapshot records reference model
SHA-256 `47a2b22f7589bcf0b4753663917bbfcfc031442efa3ddb3697fd401c4acfaaa6`;
it does not verify the model served for each response.

**Operationalization and results:** Match exact user/movie IDs at strictly later
timestamps, before one hour elapses. Duplicate source records and identical
user/time/ranked-slate responses count once; minute chunks do not increase hits.
Each response has the full follow-up duration inside both partitions' observed
time spans. **7,553 / 34,357 responses hit: 21.98% overall.**
Among **11,642 responses with observed watching**, **64.88%** hit.
The other **22,715 responses (66.11%)** have no observed watch.
Conditional HitRate is undefined if no response has observed watching.

### Runtime cost

**Metric, data and operationalization:** Sum saved catalog `license_cost` fees
once per distinct observed user/movie pair during **03:15–05:00 UTC**, including
unmatched watches. Minute chunks and repeated viewings of the same pair are not
separate units. Of **51,578 units**, **41,966 (81.36%)** have fees, totaling
**USD 13,783.43**. The **9,612 units** lacking catalog metadata have unknown cost,
so this is a covered-unit subtotal, not a complete licensing total.

For the same window, **1,794 exported LLM attempts** recorded **1,115,938 input
tokens**, **0 output tokens**, and **0 cached-input/cache-write tokens**.
Deduplicate by attempt ID; distinct retries and failed attempts with usage count
once each. Application profile reuse adds no provider attempt. Missing usage
is unknown, not zero; all selected attempts have usage.

**LLM cost metric:** Estimated USD for provider-recorded Decisions usage in the
same window. **Data:** The supplied October 29 minute-level provider export
contains 105 complete one-minute buckets, **1,793 requests** and **1,115,057
uncached input tokens** for `gpt-6-luna` on the default tier. Output and cache
counts are zero. **Operationalization:** Sum the buckets starting at or after
03:15 and before 05:00 UTC, then calculate
`1,115,057 / 1,000,000 * USD 0.10 = USD 0.1115057` (about **USD 0.112**).
[OpenAI Decisions pricing](https://developers.openai.com/api/docs/guides/decisions#pricing-and-availability),
checked **November 8, 2026**, charges USD 0.10 per million uncached input
tokens and does not charge output tokens. This uses published standard rates.

**Limitations:** Capped exports and latest-position watch retention omit history.
No observed watch does not prove inactivity; later positions can continue an
earlier viewing. Repeated viewing sessions cannot be reconstructed.
One watch may match multiple responses, so neither accuracy nor licensing
establishes causal attribution. LLM totals cover recorded attempts, not uncapped
billing.

[Evaluator](src/evaluate_online.py), [matching/cost tests](tests/test_online_evaluation.py),
[run/results instructions](DEPLOYMENT.md#read-only-online-accuracy-and-cost-evaluation-issue-32),
and [CI workflow](.github/workflows/ci.yml).
All 34 evaluator tests pass with 95% line/branch coverage; the full local suite
passes 481 tests with 89% overall coverage.
