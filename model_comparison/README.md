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

## Tests
```bash
.venv/bin/python -m pytest
```
