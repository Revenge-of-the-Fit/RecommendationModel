"""Top-k ranking metrics using Helixan's definitions."""
import numpy as np
import pandas as pd


class RankingEvaluator:
    def __init__(self, top_k: int = 10, relevance_rating: int = 7):
        if top_k <= 0:
            raise ValueError("The number of recommendations must be positive")
        if relevance_rating < 1 or relevance_rating > 10:
            raise ValueError("Relevance rating must be between 1 and 10")
        self.top_k = top_k
        self.relevance_rating = relevance_rating

    def _relevant(self, held_out: pd.DataFrame) -> dict[int, set[str]]:
        # NaN ratings compare False, so watch-only rows are never relevant
        positives = held_out[held_out["rating"] >= self.relevance_rating]
        grouped = positives.groupby("user_id")["movie_id"].agg(set)
        return {int(user_id): movies for user_id, movies in grouped.items()}

    def users_with_relevant(self, held_out: pd.DataFrame) -> list[int]:
        return sorted(self._relevant(held_out))

    def evaluate(
        self,
        recommendations: dict[int, list[str]],
        held_out: pd.DataFrame,
        catalog_size: int,
        user_ids: list[int] | None = None,
    ) -> dict:
        relevant = self._relevant(held_out)
        if user_ids is not None:
            wanted = set(user_ids)
            relevant = {user: movies for user, movies in relevant.items() if user in wanted}
        if not relevant:
            raise ValueError("The held-out data has no relevant movie ratings")

        precision, recall, ndcg, hit = [], [], [], []
        recommended, relevant_count, without_recommendations = set(), 0, 0
        for user_id, targets in relevant.items():
            movie_ids = list(recommendations.get(user_id, []))[: self.top_k]
            if not movie_ids:
                without_recommendations += 1
            hits = np.array([movie in targets for movie in movie_ids], dtype=float)
            discounts = 1.0 / np.log2(np.arange(2, len(hits) + 2))
            ideal_count = min(len(targets), self.top_k)
            ideal_dcg = (1.0 / np.log2(np.arange(2, ideal_count + 2))).sum()
            hit_count = int(hits.sum())
            precision.append(hit_count / self.top_k)
            recall.append(hit_count / len(targets))
            ndcg.append(float((hits * discounts).sum() / ideal_dcg))
            hit.append(float(hit_count > 0))
            recommended.update(movie_ids)
            relevant_count += len(targets)

        return {
            "evaluated_users": len(relevant),
            "users_without_recommendations": without_recommendations,
            "relevant_interactions": relevant_count,
            "precision_at_k": float(np.mean(precision)),
            "recall_at_k": float(np.mean(recall)),
            "ndcg_at_k": float(np.mean(ndcg)),
            "hit_rate_at_k": float(np.mean(hit)),
            "catalog_coverage": len(recommended) / catalog_size,
        }


def finalize_recommendations(
    recommendations: dict[int, list[str]],
    seen: dict[int, set[str]],
    catalog_ids: set[str],
    k: int,
) -> tuple[dict[int, list[str]], dict]:
    """Drop unknown and already-seen IDs, keep the first k. Counts what was dropped."""
    cleaned, unknown, seen_dropped = {}, 0, 0
    for user_id, movie_ids in recommendations.items():
        already = seen.get(user_id, set())
        kept = []
        for movie_id in movie_ids:
            if movie_id not in catalog_ids:
                unknown += 1
            elif movie_id in already:
                seen_dropped += 1
            else:
                kept.append(movie_id)
        cleaned[user_id] = kept[:k]
    return cleaned, {"unknown_ids_dropped": unknown, "seen_dropped": seen_dropped}
