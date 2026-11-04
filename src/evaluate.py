import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from dataset import DATA_DIRECTORY, MovieDataset
from evaluation import InteractionSplitter, RankingEvaluator
from recommender import DEFAULT_MIN_RATING, DEFAULT_REGULARIZATION, MODEL_PATH, EaseRecommender


def print_metrics(label: str, metrics: dict, top_k: int) -> None:
    print(
        f"{label}: "
        f"NDCG@{top_k}={metrics['ndcg_at_k']:.4f}, "
        f"Recall@{top_k}={metrics['recall_at_k']:.4f}, "
        f"Precision@{top_k}={metrics['precision_at_k']:.4f}",
        flush=True,
    )


def select_model(
    training: pd.DataFrame,
    validation: pd.DataFrame,
    movies: pd.DataFrame,
    evaluator: RankingEvaluator,
    regularizations: list[float],
    min_ratings: list[int],
) -> tuple[dict, list[dict]]:
    if not regularizations or not min_ratings:
        raise ValueError("Provide at least one regularization and training rating threshold")

    # Only the training threshold changes; relevance stays fixed in the evaluator
    candidates = [
        (regularization, min_rating)
        for min_rating in sorted(set(min_ratings))
        for regularization in sorted(set(regularizations))
    ]
    for regularization, min_rating in candidates:
        EaseRecommender(regularization, min_rating)

    results = []
    for regularization, min_rating in candidates:
        model = EaseRecommender(regularization, min_rating)
        model.fit(training, movies)
        metrics = evaluator.evaluate(model, validation)
        results.append(
            {"regularization": regularization, "min_rating": min_rating, "metrics": metrics}
        )
        print_metrics(
            f"Validation EASE, min_rating={min_rating}, regularization={regularization:g}",
            metrics,
            evaluator.top_k,
        )

    best = max(
        results,
        key=lambda result: (
            result["metrics"]["ndcg_at_k"],
            result["metrics"]["recall_at_k"],
            result["regularization"],
            result["min_rating"],
        ),
    )
    return best, results


def evaluate_fixed_parameters(
    training: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    movies: pd.DataFrame,
    evaluator: RankingEvaluator,
    regularization: float,
    min_rating: int,
) -> dict:
    # Parameters are already fixed, so validation rows can join training without selection.
    model = EaseRecommender(regularization, min_rating)
    model.fit(pd.concat([training, validation], ignore_index=True), movies)
    metrics = evaluator.evaluate(model, test)
    print_metrics("Test EASE (fixed parameters)", metrics, evaluator.top_k)
    return {
        "evaluation_mode": "fixed_parameters",
        "model_parameters": {"regularization": regularization, "min_rating": min_rating},
        "selection_rule": "Parameters fixed before evaluation; no tuning or model comparison",
        "test_evaluated": True,
        "test_training_data": "training and validation interactions",
        "test_model": metrics,
    }


def save_result(output: Path, result: dict) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(f"Evaluation saved to {output.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate and tune the movie recommender.")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIRECTORY)
    parser.add_argument("--output", type=Path, default=MODEL_PATH.parent / "evaluation.json")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--relevance-rating", type=int, default=7)
    parser.add_argument(
        "--min-ratings", "--min-rating", type=int, nargs="+",
        help="Training thresholds to compare; held-out relevance stays fixed.",
    )
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument(
        "--regularizations", type=float, nargs="+",
    )
    parser.add_argument(
        "--validation-only", action="store_true",
        help="Select parameters without fitting or evaluating on the final test split.",
    )
    parser.add_argument(
        "--fixed-parameters", action="store_true",
        help="Evaluate once without tuning; defaults to the production EASE parameters.",
    )
    arguments = parser.parse_args()

    if arguments.fixed_parameters:
        if arguments.validation_only:
            parser.error("--fixed-parameters cannot be combined with --validation-only")
        arguments.regularizations = arguments.regularizations or [DEFAULT_REGULARIZATION]
        arguments.min_ratings = arguments.min_ratings or [DEFAULT_MIN_RATING]
        if len(arguments.regularizations) != 1 or len(arguments.min_ratings) != 1:
            parser.error("--fixed-parameters requires exactly one regularization and minimum rating")
    else:
        arguments.regularizations = arguments.regularizations or [1, 10, 25, 50, 100, 250, 500]
        arguments.min_ratings = arguments.min_ratings or [6, 7, 8]

    try:
        splitter = InteractionSplitter(
            arguments.validation_fraction, arguments.test_fraction, arguments.seed
        )
        evaluator = RankingEvaluator(arguments.top_k, arguments.relevance_rating)
        dataset = MovieDataset(arguments.data_dir)
        training, validation, test = splitter.split(dataset.get_interactions())
        print(
            f"Interactions: train={len(training):,}, "
            f"validation={len(validation):,}, test={len(test):,}",
            flush=True,
        )

        file_hashes = {}
        for filename in ["events.csv.gz", "users.csv.gz", "movies.csv.gz"]:
            with (arguments.data_dir / filename).open("rb") as data_file:
                file_hashes[filename] = hashlib.file_digest(data_file, "sha256").hexdigest()

        result = {
            "split_method": "seeded random holdout within each user, grouped by user/movie",
            "seed": arguments.seed,
            "validation_fraction": arguments.validation_fraction,
            "test_fraction": arguments.test_fraction,
            "interaction_counts": {
                "training": len(training), "validation": len(validation), "test": len(test),
            },
            "cold_start_users_not_evaluated": len(dataset.get_cold_start_users()),
            "top_k": arguments.top_k,
            "relevance_rating": arguments.relevance_rating,
            "data_sha256": file_hashes,
        }

        if arguments.fixed_parameters:
            result.update(evaluate_fixed_parameters(
                training, validation, test, dataset.movies, evaluator,
                arguments.regularizations[0], arguments.min_ratings[0],
            ))
            save_result(arguments.output, result)
            return

        baseline = EaseRecommender(regularization=100, min_rating=7)
        baseline.fit(training, dataset.movies)
        validation_popularity = evaluator.evaluate(baseline, validation, popularity_only=True)
        validation_baseline = evaluator.evaluate(baseline, validation)
        print_metrics("Validation popularity", validation_popularity, arguments.top_k)
        print_metrics("Validation baseline EASE", validation_baseline, arguments.top_k)

        best, validation_results = select_model(
            training, validation, dataset.movies, evaluator,
            arguments.regularizations, arguments.min_ratings,
        )
        selected_regularization = best["regularization"]
        selected_min_rating = best["min_rating"]
        print(
            f"Selected regularization={selected_regularization:g}, "
            f"min_rating={selected_min_rating}",
            flush=True,
        )

        result.update({
            "selection_rule": (
                "validation NDCG, then recall, then higher regularization, then higher training threshold"
            ),
            "baseline_parameters": {"regularization": 100, "min_rating": 7},
            "validation_popularity": validation_popularity,
            "validation_baseline": validation_baseline,
            "validation_results": validation_results,
            "selected_regularization": selected_regularization,
            "selected_min_rating": selected_min_rating,
            "test_evaluated": False,
        })

        # Use the test split only after parameter selection
        if not arguments.validation_only:
            final_training = pd.concat([training, validation], ignore_index=True)
            final_model = EaseRecommender(selected_regularization, selected_min_rating)
            final_model.fit(final_training, dataset.movies)
            baseline.fit(final_training, dataset.movies)
            test_popularity = evaluator.evaluate(baseline, test, popularity_only=True)
            test_baseline = evaluator.evaluate(baseline, test)
            test_model = evaluator.evaluate(final_model, test)
            print_metrics("Test popularity", test_popularity, arguments.top_k)
            print_metrics("Test baseline EASE", test_baseline, arguments.top_k)
            print_metrics("Test selected EASE", test_model, arguments.top_k)
            result.update(
                {
                    "test_evaluated": True,
                    "test_training_data": "training and validation interactions",
                    "test_popularity": test_popularity,
                    "test_baseline": test_baseline,
                    "test_model": test_model,
                }
            )

        save_result(arguments.output, result)
    except (FileNotFoundError, ValueError) as error:
        parser.exit(1, f"Error: {error}\n")

    print(
        f"Train on all interactions with --regularization {selected_regularization:g} "
        f"--min-rating {selected_min_rating}"
    )


if __name__ == "__main__":
    main()
