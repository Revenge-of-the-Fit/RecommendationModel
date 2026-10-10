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


def _bytes(value) -> str:
    if value is None:
        return "n/a"
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{value:.0f} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024


def _number(value, template: str) -> str:
    return "n/a" if value is None else template.format(value)


def render_costs(payload: dict) -> list[str]:
    config, machine = payload["config"], payload.get("machine", {})
    lines = [
        "## Cost",
        "",
        f"Measured on {machine.get('platform', '?')} ({machine.get('processor', '?')}), "
        f"{machine.get('cpu_count', '?')} CPUs, {_bytes(machine.get('memory_bytes'))} RAM, "
        f"Python {machine.get('python', '?')}. Each model is fit once on train+validation"
        + (f" (median of {config['cost_repeats']} fits)" if config.get("cost_repeats", 1) > 1 else "")
        + "; latency is one single-user top-"
        f"{config['top_k']} request at a time on the fitted model.",
        "",
        "| Model | Fit time | Fit memory growth | Peak memory | p50 | p95 | Max | Requests/s | Model size | Timed requests |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for name, result in payload["models"].items():
        c = result.get("costs") or {}
        lines.append(
            f"| {name} | {_number(c.get('fit_seconds'), '{:.2f} s')} | {_bytes(c.get('fit_memory_growth_bytes'))} "
            f"| {_bytes(c.get('peak_rss_bytes'))} | {_number(c.get('latency_p50_ms'), '{:.1f} ms')} "
            f"| {_number(c.get('latency_p95_ms'), '{:.1f} ms')} | {_number(c.get('latency_max_ms'), '{:.1f} ms')} "
            f"| {_number(c.get('throughput_per_s'), '{:.1f}')} | {_bytes(c.get('model_size_bytes'))} "
            f"| {c.get('requests', 0)} |"
        )
    lines += ["", "Hyperparameters measured:"]
    for name, result in payload["models"].items():
        params = ", ".join(f"{k}={v}" for k, v in result["selected_params"].items()) or "-"
        lines.append(f"- **{name}**: {params}")
    lines += ["", "Model size notes:"]
    for name, result in payload["models"].items():
        lines.append(f"- **{name}**: {(result.get('costs') or {}).get('model_size_note') or 'n/a'}")
    return lines


def render_markdown(payload: dict) -> str:
    config, split = payload["config"], payload["split"]
    if config.get("only_costs"):
        return "# Model costs\n\n" + "\n".join(render_costs(payload)) + "\n"
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
    ]
    if any(result.get("costs") for result in payload["models"].values()):
        lines += [*render_costs(payload), ""]
    lines.append("## Notes")
    for name, result in payload["models"].items():
        drops = result["drops"]
        lines.append(
            f"- **{name}**: dropped {drops['unknown_ids_dropped']} unknown and "
            f"{drops['seen_dropped']} already-seen recommendations; notes: {result['notes'] or '-'}"
        )
        for user_id, reason in result["failed_users"].items():
            lines.append(f"  - user {user_id}: {reason}")
    return "\n".join(lines) + "\n"


def write_report(results_dir: Path, payload: dict, stem: str = "comparison") -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / f"{stem}.json").write_text(
        json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (results_dir / f"{stem}.md").write_text(render_markdown(payload), encoding="utf-8")
