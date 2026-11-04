# Offline evaluation — Milestone 2

Work for [#34](https://github.com/Revenge-of-the-Fit/RecommendationModel/issues/34)
and [#33](https://github.com/Revenge-of-the-Fit/RecommendationModel/issues/33).

## Run

Use the existing Python environment with `requirements.txt` installed. The command
reuses [the splitter and metrics](src/evaluation.py) and [EASE](src/recommender.py).
`--fixed-parameters` fits once with the production settings; it skips parameter
selection and baseline comparisons. It saves metrics, configuration, split counts,
and input checksums, without saving a model artifact.

The recent production export documented in [deployment](DEPLOYMENT.md#retrain-on-recent-production-data)
is `/app/models/updates/20261028T051943Z-1887626/data` inside the API container.
Copy its three CSV files and manifest into the ignored `data/m2-recent/` directory:

```bash
mkdir -p data/m2-recent
ssh <andrew-id>@CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU \
  'docker exec recommendationmodel-api-1 tar -C /app/models/updates/20261028T051943Z-1887626/data -cf - events.csv.gz users.csv.gz movies.csv.gz dataset-manifest.json' \
  | tar -xf - -C data/m2-recent

python src/evaluate.py --fixed-parameters \
  --data-dir data/m2-recent --output reports/offline-evaluation.json \
  --regularizations 100 --min-rating 6 \
  --top-k 10 --relevance-rating 7 --seed 42 \
  --validation-fraction 0.2 --test-fraction 0.2
```

Keep the copied production data outside Git. Record the manifest's actual dates
and sampling limits when reporting that run. The export is a bounded sample of
active users and prior-catalog movies, rather than the complete day's traffic.
Compare the result's `data_sha256` values with the corresponding manifest entries.

## M2 report contribution

**Metric:** Mean NDCG@10 rewards relevant movies near the top. Relevance is a
held-out rating of at least 7. Sum `relevance / log2(rank + 1)` over the top ten,
divide by the ideal sum for `min(10, relevant movies)`, then average equally
across users with a relevant test rating.

**Data:** The saved `movielog2` export contains 13,619 events, 5,000 users,
2,000 movies, and 8,253 user/movie pairs. Its requested window is October 27
05:19:45.326394–October 28 05:19:45.326394 UTC; selected events actually span
October 28 **02:32:40–05:09:30 UTC**. Copied files matched the manifest checksums.

**Operationalization:** Aggregate watches and the latest rating by user/movie
before splitting, keeping dependent observations together. Sort pairs by
user/movie and permute each user's history with one NumPy `default_rng(42)`.
For histories of at least three pairs, reserve `max(1, floor(0.2 * pair_count))`
pairs for validation and the next same-sized block for test. Shorter histories
stay in training. With parameters fixed, combine training and validation:
**8,101 fitting pairs and 152 test pairs**. Fit once with production regularization
**100** and positive-rating threshold **6**, with no tuning.

Binary profiles, weights, popularity, and seen-item exclusions use fitting data
only. Rank unseen movies by score, then fitting popularity, then movie ID.
The catalog is fixed; unsupported relevant test targets still lower NDCG.
Implementation: [command](src/evaluate.py), [split/metric](src/evaluation.py),
and [preparation](src/dataset.py). The command above saves configuration, input
hashes, and full-precision results in `reports/offline-evaluation.json`.

**Result:** On November 3, 2026, NDCG@10 was **0.170877 across 45 users**.
We excluded 107 test users without relevant ratings; 4,848 users with short
histories stayed in training. All evaluated users had positive fitting history.

**Limits:** This measures existing-user preferences; it does not evaluate future
traffic or LLM cold start. Shared users/movies create dependence even after pair
grouping. The export favors active users and a prior-model catalog, with caps of
200,000 source records, 5,000 users, and 2,000 movies. Its small scored sample and
retained-watch sampling limit generalization. We fit a fresh evaluation model
using production settings and the fitting pairs only.
