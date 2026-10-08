"""rec-zilla's item-similarity recommender, built from the training split only.

Run as submitted: its combined matrix includes MovieLens ratings from separate users.
The neighborhood size is tuned by overriding predict_rating's default argument.
"""
import sys
from pathlib import Path

import pandas as pd

from model_comparison.job import Job, JobResult

NAME = "rec_zilla"
REPO = "rec-zilla"
FULL_POPULATION = True
PARAM_GRID = [{"neighborhood_size": n} for n in (5, 10, 25)]


def recommend(job: Job) -> JobResult:
    sys.path.insert(0, job.repo_dir)
    from model.data_client import DataClient
    from model.recommender import Recommender

    client = DataClient.__new__(DataClient)  # skip __init__: it downloads and loads all data
    client.DATA_DIR = Path(job.data_dir)
    client.download_movielens_data()  # no-op when ml-latest-small is already present
    client.movies = job.movies
    client.movielens_links = pd.read_csv(client.DATA_DIR / "ml-latest-small" / "links.csv")
    client.movielens_ratings = pd.read_csv(client.DATA_DIR / "ml-latest-small" / "ratings.csv")
    client.users = job.users
    client.ratings = job.events[job.events["event_type"] == "rating"][["user_id", "movie_id", "rating"]]
    client.ratings_matrix = client.make_combined_ratings_matrix()

    class TunedRecommender(Recommender):
        def predict_rating(self, user_id, movie_id, neighborhood_size=None):
            return super().predict_rating(user_id, movie_id, job.params["neighborhood_size"])

    model = TunedRecommender(client)
    recommendations, failed = {}, {}
    for user_id in job.user_ids:
        user_id = int(user_id)
        try:
            recommendations[user_id] = [str(m) for m in model.recommend(f"course_{user_id}", n=job.k).index]
        except KeyError:
            recommendations[user_id] = []
            failed[user_id] = "no ratings in training data"
    return JobResult(recommendations, failed)
