# Offline evaluation — Milestone 2

The goal was to check how well our existing recommender suggests movies people
like. The work had two parts: prepare a repeatable check, then run it and explain
the results.

Imagine someone liked movies A, B, and C. We hide their interaction with C, train
using the remaining history, and ask for ten recommendations. We then check
whether C appears and how close it is to the top. This is an offline evaluation:
we use recorded history, keeping some examples out of training to check the model.

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

We can now report the dates and selection limits of the data we evaluated. 
Our results come from a selected subset of users and movies.

[Offline evaluation report section](m2_report.md#offline-evaluation).
