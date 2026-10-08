"""Load the course tables and build the user-movie interaction table."""
import hashlib
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

COURSE_DATA_URL = (
    "https://github.com/mlip-cmu-online/public-data/raw/refs/heads/main/m0/data/"
)
DATA_FILES = ("events.csv.gz", "users.csv.gz", "movies.csv.gz")
KEYS = ["user_id", "movie_id"]
MOVIE_EVENTS = ["watch", "rating"]


@dataclass
class Tables:
    events: pd.DataFrame
    users: pd.DataFrame
    movies: pd.DataFrame


def fetch_course_data(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for name in DATA_FILES:
        destination = data_dir / name
        if not destination.exists():
            urllib.request.urlretrieve(COURSE_DATA_URL + name, destination)


def load_tables(data_dir: Path) -> Tables:
    for name in DATA_FILES:
        if not (data_dir / name).is_file():
            raise FileNotFoundError(f"Missing {data_dir / name}; run fetch_course_data first")
    return Tables(
        events=pd.read_csv(data_dir / "events.csv.gz"),
        users=pd.read_csv(data_dir / "users.csv.gz"),
        movies=pd.read_csv(data_dir / "movies.csv.gz"),
    )


def build_interactions(events: pd.DataFrame) -> pd.DataFrame:
    movie_events = events[events["event_type"].isin(MOVIE_EVENTS)].copy()
    movie_events["watch_count"] = (movie_events["event_type"] == "watch").astype("int64")
    interactions = movie_events.groupby(KEYS, as_index=False).agg(
        watch_count=("watch_count", "sum"),
        first_timestamp=("timestamp", "min"),
        last_timestamp=("timestamp", "max"),
    )
    ratings = movie_events[movie_events["event_type"] == "rating"]
    # Timestamps are ISO strings, so sorting them sorts by time; keep the latest rating
    ratings = ratings.sort_values("timestamp", kind="stable").drop_duplicates(KEYS, keep="last")
    interactions = interactions.merge(
        ratings[KEYS + ["rating"]], on=KEYS, how="left", validate="one_to_one"
    )
    interactions["rating"] = interactions["rating"].astype("float64")
    interactions = interactions.sort_values(KEYS).reset_index(drop=True)
    return interactions[KEYS + ["watch_count", "rating", "first_timestamp", "last_timestamp"]]


def events_for_pairs(events: pd.DataFrame, interactions: pd.DataFrame) -> pd.DataFrame:
    """Watch/rating events whose user and movie pair appears in `interactions`."""
    pairs = interactions[KEYS].drop_duplicates()
    movie_events = events[events["event_type"].isin(MOVIE_EVENTS)]
    return movie_events.merge(pairs, on=KEYS, how="inner").reset_index(drop=True)


def seen_by_user(interactions: pd.DataFrame) -> dict[int, set[str]]:
    grouped = interactions.groupby("user_id")["movie_id"].agg(set)
    return {int(user_id): movies for user_id, movies in grouped.items()}


def file_sha256(data_dir: Path) -> dict[str, str]:
    hashes = {}
    for name in DATA_FILES:
        with (data_dir / name).open("rb") as handle:
            hashes[name] = hashlib.file_digest(handle, "sha256").hexdigest()
    return hashes
