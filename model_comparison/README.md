# Model comparison harness

Scores every team model with one shared held-out protocol (seeded per-user 60/20/20 split,
tune on validation, refit on train+validation, score on test once; relevant = rating >= 7, k = 10).

## Setup
```bash
.venv/bin/pip install -r requirements-eval.txt
scripts/setup_external.sh        # clones the four model repos into external/ (read-only use)
echo 'OPENAI_API_KEY=...' >> .env   # only needed for the d-urbonas model
```

## Run
```bash
# cheap dry run: ~20 tuning users, 5 d-urbonas users (about 10 OpenAI requests)
.venv/bin/python -m model_comparison.compare --tuning-users 20 --d-urbonas-sample 5

# full run
.venv/bin/python -m model_comparison.compare
```
Output: `results/comparison.md` and `results/comparison.json`. d-urbonas responses are cached in
`.cache/d-urbonas/`; delete that folder if the model or its embeddings change.

Run a subset with `--models popularity Helixan`.

The d-urbonas model needs OpenAI credit. If the account has none, the run stops with a clear
message (completed users stay cached, so a rerun resumes).

## Costs (training, inference, size)
Every full run also reports cost, measured on the final fit (train+validation) of each model:

| Quality | Metric | How |
|---|---|---|
| Training cost | fit seconds; memory growth during fit; peak process memory | wall clock around the model's fit/train call, in the adapter's own process; RSS before/after via psutil; peak from `getrusage` |
| Inference cost | p50 / p95 / max latency of one single-user top-10 request; requests per second | each user's request timed alone on the fitted model (requests/s = 1 / mean latency) |
| Size | serialized bytes of the trained model | `len(pickle.dumps(model))` (d-urbonas: size of its embeddings file) |

To measure cost only (no tuning, no scoring), for example on the course VM:
```bash
.venv/bin/python -m model_comparison.compare --only-costs --cost-users 200 --cost-repeats 3
```
This writes `results/costs.md` and `results/costs.json` and leaves `comparison.*` untouched. It reuses each
model's hyperparameters from an earlier `results/comparison.json` when there is one, otherwise the middle of
its grid, and lists them in the report. `--cost-repeats N` refits local models N times and reports the median
fit time (d-urbonas is never repeated: it would repeat paid API calls).

Things to know when quoting the numbers:
- They describe the machine printed above the table; the deployed VM will differ.
- d-urbonas has no fit when its precomputed embeddings are reused ("n/a"); its latency is the real
  LLM + embedding request time, stored in its cache so reruns still report it.
- Model size for MajorTomLanded includes the MovieLens data its model loads, as submitted.
- Latency is per request on a warm model; it excludes model loading and any HTTP layer.

## Tests
```bash
.venv/bin/python -m pytest
```
