"""Small synthetic course-format data set for tests; no production data or network."""
import gzip
from pathlib import Path

import pandas as pd


MOVIE_COLUMNS = ["movie_id", "title", "genres", "overview", "runtime", "license_cost"]
USER_COLUMNS = ["user_id", "self_description_likes", "self_description_dislikes"]
EVENT_COLUMNS = ["timestamp", "user_id", "event_type", "movie_id", "rating"]


def sample_movies() -> pd.DataFrame:
    return pd.DataFrame(
        [(letter, f"Movie {letter}", "Drama", "A film.", "100", "0.5") for letter in "abcdef"],
        columns=MOVIE_COLUMNS,
    )


def sample_users() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("1", "", ""), ("2", "", ""), ("3", "", ""), ("4", "", ""),
            ("1001", "quiet dramas", "gore"), ("1002", "", ""),
        ],
        columns=USER_COLUMNS,
    )


def sample_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("2026-01-01T10:00:00Z", "1", "watch", "a", ""),
            ("2026-01-01T10:05:00Z", "1", "rating", "a", "6"),
            ("2026-01-02T10:00:00Z", "1", "rating", "b", "5"),
            ("2026-01-02T11:00:00Z", "1", "watch", "c", ""),
            ("2026-01-03T10:00:00Z", "2", "rating", "a", "8"),
            ("2026-01-03T10:10:00Z", "2", "rating", "d", "8"),
            ("2026-01-03T10:20:00Z", "2", "rating", "e", "6"),
            ("2026-01-04T10:00:00Z", "3", "rating", "b", "9"),
            ("2026-01-04T10:10:00Z", "3", "rating", "d", "7"),
            ("2026-01-04T10:20:00Z", "3", "rating", "d", "9"),
            ("2026-01-05T10:00:00Z", "3", "watch", "d", ""),
            ("2026-01-05T11:00:00Z", "3", "watch", "d", ""),
            ("2026-01-05T12:00:00Z", "4", "rating", "a", "5"),
            ("2026-02-01T09:00:00Z", "1001", "account_created", "", ""),
            ("2026-02-01T09:01:00Z", "1002", "account_created", "", ""),
        ],
        columns=EVENT_COLUMNS,
    )


def write_table(directory: Path, filename: str, table: pd.DataFrame) -> None:
    with gzip.open(directory / filename, "wt", encoding="utf-8", newline="") as output:
        table.to_csv(output, index=False)


def write_sample_data(directory: Path, events=None, users=None, movies=None) -> Path:
    """Write the three course files, optionally replacing any table."""
    directory.mkdir(parents=True, exist_ok=True)
    write_table(directory, "events.csv.gz", sample_events() if events is None else events)
    write_table(directory, "users.csv.gz", sample_users() if users is None else users)
    write_table(directory, "movies.csv.gz", sample_movies() if movies is None else movies)
    return directory
