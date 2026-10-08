import pandas as pd

from model_comparison.data import build_interactions, events_for_pairs, seen_by_user


def make_events():
    return pd.DataFrame(
        {
            "timestamp": ["2025-01-01T00:00:00", "2025-01-01T00:01:00", "2025-01-01T00:02:00",
                          "2025-01-01T00:03:00", "2025-01-01T00:00:00"],
            "user_id": [1, 1, 1, 1, 2],
            "event_type": ["watch", "rating", "rating", "watch", "account_created"],
            "movie_id": ["a", "a", "a", "b", None],
            "rating": [None, 4.0, 9.0, None, None],
        }
    )


def test_interactions_keep_latest_rating_and_watch_count():
    interactions = build_interactions(make_events())
    row_a = interactions[interactions["movie_id"] == "a"].iloc[0]
    row_b = interactions[interactions["movie_id"] == "b"].iloc[0]
    assert row_a["rating"] == 9.0 and row_a["watch_count"] == 1
    assert pd.isna(row_b["rating"]) and row_b["watch_count"] == 1
    assert len(interactions) == 2  # account_created is not an interaction


def test_events_for_pairs_filters_to_given_pairs():
    events = make_events()
    interactions = build_interactions(events)
    only_a = interactions[interactions["movie_id"] == "a"]
    kept = events_for_pairs(events, only_a)
    assert set(kept["movie_id"]) == {"a"}
    assert set(kept["event_type"]) == {"watch", "rating"}


def test_seen_by_user():
    seen = seen_by_user(build_interactions(make_events()))
    assert seen == {1: {"a", "b"}}
