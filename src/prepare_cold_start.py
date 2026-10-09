import argparse
import os
from pathlib import Path

from cold_start import ColdStartRecommender
from preferences import CACHE_DIRECTORY, ColdStartError
from recommender import MODEL_PATH, EaseRecommender
from dataset import DATA_DIRECTORY, MovieDataset
from services.versions import dataset_version, file_version
from storage.database import DEFAULT_STORAGE_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="Cache LLM preference profiles for cold-start users.")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIRECTORY)
    parser.add_argument("--cache-dir", type=Path, default=CACHE_DIRECTORY)
    parser.add_argument("--model-path", type=Path, default=MODEL_PATH)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--limit", type=int)
    arguments = parser.parse_args()

    try:
        if arguments.limit is not None and arguments.limit <= 0:
            raise ValueError("The user limit must be positive")

        if arguments.top_k < 1:
            raise ValueError("The number of recommendations must be positive")

        dataset = MovieDataset(arguments.data_dir)
        model = EaseRecommender.load(arguments.model_path)
        users = dataset.users.loc[
            ~dataset.users["user_id"].map(model.has_positive_history)
        ]
        if arguments.limit is not None:
            users = users.head(arguments.limit)

        recommender = ColdStartRecommender(
            dataset.movies, arguments.cache_dir, model=model, storage_path=arguments.storage_path,
        )
        input_versions = {
            "dataset": dataset_version(arguments.data_dir), "model": file_version(arguments.model_path),
        }
        generated_count = 0
        cached_count = 0

        for user in users.itertuples():
            likes = user.self_description_likes
            dislikes = user.self_description_dislikes

            if not likes.strip() and not dislikes.strip():
                print(f"User {user.user_id}: no description; popularity fallback", flush=True)
                continue

            result = recommender.recommend(
                likes, dislikes, arguments.top_k,
                context={"user_id": int(user.user_id), "input_versions": input_versions},
            )
            cached_count += int(result["cached"])
            generated_count += int(not result["cached"])
            source = "cache" if result["cached"] else "GPT6 Luna"
            print(
                f"User {user.user_id}: {len(result['recommendations'])} recommendations ({source})",
                flush=True,
            )
    except (FileNotFoundError, ValueError, ColdStartError) as error:
        parser.exit(1, f"Error: {error}\n")

    print(f"Generated: {generated_count}; reused from cache: {cached_count}")


if __name__ == "__main__":
    main()
