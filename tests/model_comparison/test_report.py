from model_comparison.report import render_markdown

METRICS = {"evaluated_users": 10, "users_without_recommendations": 1, "relevant_interactions": 30,
           "precision_at_k": 0.1, "recall_at_k": 0.2, "ndcg_at_k": 0.3, "hit_rate_at_k": 0.4,
           "catalog_coverage": 0.5}


def payload():
    return {
        "config": {"seed": 42, "top_k": 10, "relevance_rating": 7},
        "split": {"training": 100, "validation": 20, "test": 20, "test_users_evaluable": 10,
                  "sample_users": 5, "test_users_without_description": 2},
        "data_sha256": {},
        "models": {
            "helixan": {"selected_params": {"regularization": 100}, "full_population": METRICS,
                        "shared_sample": METRICS, "drops": {"unknown_ids_dropped": 0, "seen_dropped": 0},
                        "failed_users": {}, "notes": {}},
            "urbonas": {"selected_params": {}, "full_population": None, "shared_sample": METRICS,
                        "drops": {"unknown_ids_dropped": 0, "seen_dropped": 0},
                        "failed_users": {7: "RuntimeError: boom"}, "notes": {"unmapped_titles": 3}},
        },
    }


def test_markdown_has_both_tables_and_flags_failures():
    text = render_markdown(payload())
    assert "helixan" in text and "urbonas" in text
    assert "0.3000" in text                      # ndcg formatted to 4 places
    assert "not evaluated" in text.lower()       # urbonas has no full-population row
    assert "1 failed" in text                    # failure visible in the table
