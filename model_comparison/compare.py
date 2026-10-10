"""Run every selected model through the same split and write one comparison."""
import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from model_comparison.adapters import get_adapter
from model_comparison.costs import machine_info, median_fit_seconds
from model_comparison.data import (
    Tables, build_interactions, events_for_pairs, fetch_course_data, file_sha256,
    load_tables, seen_by_user,
)
from model_comparison.job import Job
from model_comparison.metrics import RankingEvaluator, finalize_recommendations
from model_comparison.repos import resolve_repo
from model_comparison.report import write_report
from model_comparison.runner import run_job
from model_comparison.split import InteractionSplitter


def sample_users(users: list[int], n: int, seed: int) -> list[int]:
    users = sorted(users)
    if n >= len(users):
        return users
    chosen = np.random.default_rng(seed).choice(len(users), size=n, replace=False)
    return sorted(users[i] for i in chosen)


def adapter_repo_dir(name: str, external_dir: Path) -> str:
    repo = resolve_repo(get_adapter(name).REPO, external_dir)
    return str(repo.resolve()) if repo else ""


def check_prerequisites(models: list[str], external_dir: Path, env) -> None:
    # The key comes first: it is the one thing only the user can supply
    if "d-urbonas" in models and not env.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is not set; add it to .env before running d-urbonas")
    for name in models:
        repo = resolve_repo(get_adapter(name).REPO, external_dir)
        if repo and not repo.is_dir():
            raise SystemExit(f"Missing {repo}; run scripts/setup_external.sh")


def _score(result, seen, held_out, evaluator, catalog_ids, user_ids):
    cleaned, drops = finalize_recommendations(
        result.recommendations, seen, catalog_ids, evaluator.top_k
    )
    metrics = evaluator.evaluate(cleaned, held_out, len(catalog_ids), user_ids=user_ids)
    return metrics, drops


def select_params(adapter, make_job, train, validation, tuning_users, evaluator, catalog_ids, runner):
    """Pick the grid point with the best validation NDCG (ties: recall, then grid order)."""
    grid = adapter.PARAM_GRID
    if len(grid) == 1:
        return grid[0], []
    rows, seen = [], seen_by_user(train)
    for params in grid:
        result = runner(make_job(params, train, tuning_users))
        metrics, _ = _score(result, seen, validation, evaluator, catalog_ids, tuning_users)
        rows.append({"params": params, "ndcg_at_k": metrics["ndcg_at_k"], "recall_at_k": metrics["recall_at_k"]})
        print(f"  {adapter.NAME} {params}: NDCG={metrics['ndcg_at_k']:.4f}", flush=True)
    best = max(range(len(rows)), key=lambda i: (rows[i]["ndcg_at_k"], rows[i]["recall_at_k"], -i))
    return grid[best], rows


def params_for_costs(adapter, previous: dict | None) -> dict:
    """Costs depend on the hyperparameters: reuse the ones a comparison selected, else the middle of the grid."""
    if previous is not None and previous in adapter.PARAM_GRID:
        return previous
    return adapter.PARAM_GRID[len(adapter.PARAM_GRID) // 2]


def previous_params(results_dir: Path) -> dict:
    path = Path(results_dir) / "comparison.json"
    if not path.exists():
        return {}
    models = json.loads(path.read_text(encoding="utf-8")).get("models", {})
    return {name: result.get("selected_params") for name, result in models.items()}


def collect_costs(first_result, extra_results) -> dict:
    """Costs of the final job; with repeats, fit time is the median and every run is kept."""
    costs = dict(first_result.notes.get("costs", {}))
    if extra_results:
        runs = [costs, *[r.notes.get("costs", {}) for r in extra_results]]
        costs["fit_seconds_runs"] = [run.get("fit_seconds") for run in runs]
        costs["fit_seconds"] = median_fit_seconds(runs)
    return costs


def run_comparison(args, runner=run_job) -> dict:
    tables = load_tables(args.data_dir)
    interactions = build_interactions(tables.events)
    train, validation, test = InteractionSplitter(
        args.validation_fraction, args.test_fraction, args.seed
    ).split(interactions)
    evaluator = RankingEvaluator(args.top_k, args.relevance_rating)
    catalog_ids = set(tables.movies["movie_id"])
    external_dir = getattr(args, "external_dir", Path("external"))
    cache_dir = getattr(args, "cache_dir", Path(".cache"))

    tuning_users = sample_users(evaluator.users_with_relevant(validation), args.tuning_users, args.seed)
    test_users = evaluator.users_with_relevant(test)
    described = set(
        tables.users.loc[tables.users["self_description_likes"].fillna("").str.strip() != "", "user_id"]
    )
    eligible = [u for u in test_users if u in described]
    shared_sample = sample_users(eligible, args.d_urbonas_sample, args.seed)
    trainval = pd.concat([train, validation], ignore_index=True)

    def make_job(name):
        def build(params, training, users):
            return Job(
                adapter=name, params=params, interactions=training,
                events=events_for_pairs(tables.events, training), movies=tables.movies,
                users=tables.users, user_ids=[int(u) for u in users], k=args.top_k,
                repo_dir=adapter_repo_dir(name, external_dir), data_dir=str(Path(args.data_dir).resolve()),
                cache_dir=str(Path(cache_dir).resolve()),
            )
        return build

    only_costs = getattr(args, "only_costs", False)
    cost_users = sample_users(test_users, getattr(args, "cost_users", 200), args.seed)
    repeats = max(1, getattr(args, "cost_repeats", 1))
    earlier = previous_params(args.results_dir) if only_costs else {}

    models = {}
    for name in args.models:
        adapter, build = get_adapter(name), make_job(name)
        print(f"== {name}", flush=True)
        if only_costs:
            params, tuning_rows = params_for_costs(adapter, earlier.get(name)), []
            users = cost_users if adapter.FULL_POPULATION else shared_sample
        else:
            params, tuning_rows = select_params(
                adapter, build, train, validation, tuning_users, evaluator, catalog_ids, runner
            )
            users = test_users if adapter.FULL_POPULATION else shared_sample
        result = runner(build(params, trainval, users))
        # Repeating a paid-API model would repeat its paid calls, so only local models are repeated
        extra = [runner(build(params, trainval, users)) for _ in range(repeats - 1)] if adapter.FULL_POPULATION else []
        costs = collect_costs(result, extra)

        shared_metrics = full_metrics = None
        drops = {"unknown_ids_dropped": 0, "seen_dropped": 0}
        if not only_costs:
            seen = seen_by_user(trainval)
            shared_metrics, drops = _score(result, seen, test, evaluator, catalog_ids, shared_sample)
            if adapter.FULL_POPULATION:
                full_metrics, drops = _score(result, seen, test, evaluator, catalog_ids, test_users)
        models[name] = {
            "selected_params": params, "tuning": tuning_rows,
            "full_population": full_metrics, "shared_sample": shared_metrics,
            "drops": drops, "failed_users": result.failed_users,
            "notes": {k: v for k, v in result.notes.items() if k != "costs"}, "costs": costs,
        }

    payload = {
        "config": {
            "seed": args.seed, "top_k": args.top_k, "relevance_rating": args.relevance_rating,
            "validation_fraction": args.validation_fraction, "test_fraction": args.test_fraction,
            "tuning_users": len(tuning_users), "d_urbonas_sample": args.d_urbonas_sample,
            "only_costs": only_costs, "cost_users": len(cost_users), "cost_repeats": repeats,
        },
        "machine": machine_info(),
        "split": {
            "training": len(train), "validation": len(validation), "test": len(test),
            "test_users_evaluable": len(test_users), "sample_users": len(shared_sample),
            "test_users_without_description": len(test_users) - len(eligible),
        },
        "data_sha256": file_sha256(args.data_dir),
        "models": models,
    }
    # A costs-only run must not overwrite the accuracy results (or the params it reads from them)
    write_report(Path(args.results_dir), payload, stem="costs" if only_costs else "comparison")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare recommender models on a shared held-out test.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--external-dir", type=Path, default=Path("external"))
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache"))
    parser.add_argument("--results-dir", type=Path, default=Path("results"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--relevance-rating", type=int, default=7)
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--tuning-users", type=int, default=200)
    parser.add_argument("--d-urbonas-sample", type=int, default=50)
    parser.add_argument("--only-costs", action="store_true",
                        help="skip tuning and scoring; measure training cost, latency and size only "
                             "(writes costs.md/json; reuses params from an earlier comparison.json)")
    parser.add_argument("--cost-users", type=int, default=200,
                        help="users whose single requests are timed with --only-costs")
    parser.add_argument("--cost-repeats", type=int, default=1,
                        help="runs per local model; fit time is the median")
    parser.add_argument("--models", nargs="+", default=["popularity", "Helixan", "MajorTomLanded", "MuhammadDF", "d-urbonas"],
                        choices=["popularity", "Helixan", "MajorTomLanded", "MuhammadDF", "d-urbonas"])
    return parser


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    check_prerequisites(args.models, args.external_dir, os.environ)
    fetch_course_data(args.data_dir)
    payload = run_comparison(args)
    print(f"Wrote {args.results_dir / ('costs.md' if args.only_costs else 'comparison.md')}")
    for name, result in payload["models"].items():
        if result["shared_sample"] is None:
            print(f"{name}: fit {result['costs']['fit_seconds']}s, p95 {result['costs']['latency_p95_ms']} ms")
        else:
            print(f"{name}: NDCG@{args.top_k}={result['shared_sample']['ndcg_at_k']:.4f} (shared sample)")


if __name__ == "__main__":
    main()
