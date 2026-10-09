import argparse
import json
from pathlib import Path

from cold_start import ColdStartRecommender
from preferences import CACHE_DIRECTORY, ColdStartError
from dataset import DATA_DIRECTORY, MovieDataset
from recommender import MODEL_PATH, EaseRecommender


def recommend_for_user(
    model: EaseRecommender,
    user_id: int,
    top_k: int = 10,
    data_directory: Path = DATA_DIRECTORY,
    cache_directory: Path = CACHE_DIRECTORY,
    offline: bool = False,
    *,
    dataset: MovieDataset | None = None,
    cold_start: ColdStartRecommender | None = None,
) -> dict:
    """Use the existing routing, optionally reusing resources loaded by a server."""
    if user_id <= 0 or top_k <= 0:
        raise ValueError("User ID and recommendation count must be positive")

    if model.has_positive_history(user_id):
        return {
            "user_id": user_id,
            "method": "ease",
            "recommendations": model.recommend(user_id, top_k).to_dict(orient="records"),
        }

    if dataset is None:
        dataset = MovieDataset(data_directory)
    users = dataset.users.loc[dataset.users["user_id"].eq(user_id)]

    if not users.empty:
        user = users.iloc[0]
        likes = user["self_description_likes"]
        dislikes = user["self_description_dislikes"]

        if likes.strip() or dislikes.strip():
            seen = set()
            user_row = model.user_index.get(user_id)
            if user_row is not None:
                seen = set(model.movie_ids[model.seen_movies[user_row]])

            if cold_start is None:
                cold_start = ColdStartRecommender(dataset.movies, cache_directory, model=model)
            result = cold_start.recommend(likes, dislikes, top_k, seen, offline)
            return {"user_id": user_id, "method": "llm_cold_start", **result}

    return {
        "user_id": user_id,
        "method": "popularity",
        "fallback_reason": "user_not_in_dataset" if users.empty else "no_description_or_positive_history",
        "recommendations": model.recommend(user_id, top_k).to_dict(orient="records"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Recommend movies for a user.")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--model-path", type=Path, default=MODEL_PATH)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIRECTORY)
    parser.add_argument("--cache-dir", type=Path, default=CACHE_DIRECTORY)
    parser.add_argument("--offline", action="store_true", help="Use cached cold-start responses only.")
    arguments = parser.parse_args()

    try:
        model = EaseRecommender.load(arguments.model_path)
        result = recommend_for_user(
            model,
            arguments.user_id,
            arguments.top_k,
            arguments.data_dir,
            arguments.cache_dir,
            arguments.offline,
        )
    except (FileNotFoundError, ValueError, ColdStartError) as error:
        parser.exit(1, f"Error: {error}\n")

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
