import math

import pandas as pd
import pytest

from model_comparison.metrics import RankingEvaluator


def held_out(rows):
    return pd.DataFrame(rows, columns=["user_id", "movie_id", "rating"])


def test_hand_computed_metrics():
    # User 1 relevant {a, b}; recs [a, x, b]. User 2 relevant {d}; no recs.
    frame = held_out([(1, "a", 8.0), (1, "b", 9.0), (1, "c", 3.0), (2, "d", 7.0)])
    evaluator = RankingEvaluator(top_k=3, relevance_rating=7)
    result = evaluator.evaluate({1: ["a", "x", "b"]}, frame, catalog_size=10)
    ndcg_user_1 = 1.5 / (1 + 1 / math.log2(3))
    assert result["evaluated_users"] == 2
    assert result["users_without_recommendations"] == 1
    assert result["relevant_interactions"] == 3
    assert result["precision_at_k"] == pytest.approx((2 / 3 + 0) / 2)
    assert result["recall_at_k"] == pytest.approx((1.0 + 0) / 2)
    assert result["ndcg_at_k"] == pytest.approx(ndcg_user_1 / 2)
    assert result["hit_rate_at_k"] == pytest.approx(0.5)
    assert result["catalog_coverage"] == pytest.approx(3 / 10)


def test_missing_and_low_ratings_are_not_relevant():
    frame = held_out([(1, "a", None), (1, "b", 6.0), (2, "c", 7.0)])
    result = RankingEvaluator(top_k=2).evaluate({2: ["c"]}, frame, catalog_size=5)
    assert result["evaluated_users"] == 1  # user 1 has nothing relevant
    assert result["hit_rate_at_k"] == 1.0


def test_no_relevant_users_raises():
    frame = held_out([(1, "a", None), (1, "b", 6.0)])
    with pytest.raises(ValueError, match="no relevant"):
        RankingEvaluator().evaluate({}, frame, catalog_size=5)


def test_user_ids_restricts_the_evaluated_population():
    frame = held_out([(1, "a", 9.0), (2, "b", 9.0)])
    result = RankingEvaluator(top_k=1).evaluate(
        {1: ["a"], 2: ["z"]}, frame, catalog_size=4, user_ids=[1]
    )
    assert result["evaluated_users"] == 1 and result["hit_rate_at_k"] == 1.0


def test_users_with_relevant_is_sorted():
    frame = held_out([(5, "a", 9.0), (2, "b", 7.0), (3, "c", 1.0)])
    assert RankingEvaluator().users_with_relevant(frame) == [2, 5]


def test_invalid_arguments():
    with pytest.raises(ValueError):
        RankingEvaluator(top_k=0)
    with pytest.raises(ValueError):
        RankingEvaluator(relevance_rating=11)
