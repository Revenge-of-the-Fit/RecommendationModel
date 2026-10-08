from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
EXTERNAL_DIR = PROJECT_ROOT / "external"


def make_interactions(rows):
    """rows: (user_id, movie_id, rating or None, watch_count)."""
    frame = pd.DataFrame(rows, columns=["user_id", "movie_id", "rating", "watch_count"])
    frame["rating"] = frame["rating"].astype("float64")
    frame["first_timestamp"] = "2025-01-01T00:00:00"
    frame["last_timestamp"] = "2025-01-01T00:01:00"
    return frame


@pytest.fixture
def toy_interactions():
    rows = [(1, f"m{i}", float(5 + i % 5), 1) for i in range(10)]
    rows += [(2, "m0", 8.0, 1), (2, "m1", None, 1)]
    return make_interactions(rows)


@pytest.fixture(scope="session")
def real_slice():
    """First 60 users of the real course data; skipped when data/ is absent."""
    from model_comparison.data import build_interactions, events_for_pairs, load_tables

    if not (DATA_DIR / "events.csv.gz").exists():
        pytest.skip("course data not downloaded")
    tables = load_tables(DATA_DIR)
    users = sorted(tables.users["user_id"])[:60]
    events = tables.events[tables.events["user_id"].isin(users)]
    interactions = build_interactions(events)
    return {
        "tables": tables,
        "interactions": interactions,
        "events": events_for_pairs(events, interactions),
        "users": users,
    }
