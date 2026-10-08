"""Render the comparison payload as JSON and a Markdown table."""
import json
from pathlib import Path

COLUMNS = [
    ("evaluated_users", "Users", "{}"),
    ("ndcg_at_k", "NDCG@k", "{:.4f}"),
    ("recall_at_k", "Recall@k", "{:.4f}"),
    ("precision_at_k", "Precision@k", "{:.4f}"),
    ("hit_rate_at_k", "HitRate@k", "{:.4f}"),
    ("catalog_coverage", "Coverage", "{:.4f}"),
    ("users_without_recommendations", "No recs", "{}"),
]


def _table(models: dict, key: str, empty_note: str) -> list[str]:
    header = ["Model", *[label for _, label, _ in COLUMNS], "Failed", "Selected params"]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for name, result in models.items():
        metrics = result[key]
        failed = f"{len(result['failed_users'])} failed" if result["failed_users"] else "0"
        params = ", ".join(f"{k}={v}" for k, v in result["selected_params"].items()) or "-"
        if metrics is None:
            cells = [empty_note] + [""] * (len(COLUMNS) - 1)
        else:
            cells = [fmt.format(metrics[field]) for field, _, fmt in COLUMNS]
        lines.append("| " + " | ".join([name, *cells, failed, params]) + " |")
    return lines


def render_markdown(payload: dict) -> str:
    config, split = payload["config"], payload["split"]
    lines = [
        "# Model comparison",
        "",
        f"Seed {config['seed']}, top-{config['top_k']}, relevant = rating >= {config['relevance_rating']}. "
        f"Interactions: train {split['training']:,}, validation {split['validation']:,}, test {split['test']:,}. "
        "Models are tuned on validation, refit on train+validation, scored on test once.",
        "",
        f"## Shared sample ({split['sample_users']} test users, identical for every model)",
        "",
        *_table(payload["models"], "shared_sample", "not evaluated"),
        "",
        f"## Full test population ({split['test_users_evaluable']} users)",
        "",
        *_table(payload["models"], "full_population", "not evaluated (sampled model)"),
        "",
        f"Test users excluded from the shared sample for lacking a self-description: "
        f"{split['test_users_without_description']}.",
        "",
        "## Notes",
    ]
    for name, result in payload["models"].items():
        drops = result["drops"]
        lines.append(
            f"- **{name}**: dropped {drops['unknown_ids_dropped']} unknown and "
            f"{drops['seen_dropped']} already-seen recommendations; notes: {result['notes'] or '-'}"
        )
        for user_id, reason in result["failed_users"].items():
            lines.append(f"  - user {user_id}: {reason}")
    return "\n".join(lines) + "\n"


def write_report(results_dir: Path, payload: dict) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "comparison.json").write_text(
        json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (results_dir / "comparison.md").write_text(render_markdown(payload), encoding="utf-8")
