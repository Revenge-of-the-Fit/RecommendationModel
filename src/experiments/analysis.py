"""One outcome per randomized user, with explicit missing-data coverage."""
import gzip
import json
from collections import Counter
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
from scipy import stats

from evaluate_online import load_licensing_fees, sha256, utc
from experiments.config import arm_for_slot, experiment_routing, validate_plan
from routing import selected_slot


def read_jsonl(path):
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            yield json.loads(line)


def welch(control, treatment):
    control, treatment = np.asarray(control, dtype=float), np.asarray(treatment, dtype=float)
    result = {"method": "two-sided Welch t-test; independent users; treatment minus control",
              "control_users": len(control), "treatment_users": len(treatment),
              "difference": float(treatment.mean() - control.mean()) if len(control) and len(treatment) else None,
              "ci95": None, "p_value": None}
    if min(len(control), len(treatment)) < 2:
        return {**result, "status": "insufficient_users"}
    if not (np.isfinite(control).all() and np.isfinite(treatment).all()):
        raise ValueError("Statistical inputs must be finite")
    if control.var(ddof=1) + treatment.var(ddof=1) == 0:
        return {**result, "status": "no_observed_variation"}
    test = stats.ttest_ind(treatment, control, equal_var=False)
    interval = test.confidence_interval(confidence_level=0.95)
    return {**result, "status": "estimated", "ci95": [float(interval.low), float(interval.high)],
            "p_value": float(test.pvalue)}


def analyze(plan, requests, observations, fees, invalid_fees):
    validate_plan(plan)
    start, end = utc(plan["start"]), utc(plan["enrollment_end"])
    follow_up = timedelta(seconds=plan["follow_up_seconds"])
    cohort, seen, counts = {}, {}, Counter()
    routing = experiment_routing(plan)
    for row in requests:
        if row["experiment_id"] != plan["id"]:
            raise ValueError("Snapshot contains another experiment's requests")
        key = row["request_id"]
        if key in seen:
            if seen[key] != row:
                raise ValueError("Conflicting duplicate experiment request")
            continue
        seen[key] = row
        user, stamp = row["user_id"], utc(row["started_at"])
        slot = selected_slot(routing, user)
        if row["assigned_slot"] != slot or row["assigned_arm"] != arm_for_slot(plan, slot):
            raise ValueError("Recorded assignment differs from the committed user split")
        if row["status"] == 200:
            if row["actual_slot"] not in plan["versions"] or row["served_version"] != plan["versions"][row["actual_slot"]]:
                raise ValueError("Served version changed or is missing; this run did not keep its models fixed")
            counts["fallback_requests"] += row["actual_slot"] != slot
        else:
            counts["failed_requests"] += 1
        counts["requests"] += 1
        if start <= stamp < end and (user not in cohort or stamp < cohort[user]["exposure"]):
            cohort[user] = {"user_id": user, "arm": row["assigned_arm"], "exposure": stamp}
    watches, ratings = {}, {}
    previous_identity, previous_row = None, None
    for row in observations:
        identity = (row["partition"], row["offset"])
        if identity == previous_identity and row == previous_row:
            continue
        if previous_identity is not None and identity <= previous_identity:
            raise ValueError("Outcomes must have unique, ordered partition/offset coordinates")
        previous_identity, previous_row = identity, row
        user, stamp = row["user_id"], utc(row["timestamp"])
        if user not in cohort or not cohort[user]["exposure"] < stamp < cohort[user]["exposure"] + follow_up:
            continue
        key = (user, row["movie_id"])
        if row["kind"] == "watch":
            watches[key] = min(watches.get(key, stamp), stamp)
        elif row["kind"] == "rating":
            if type(row["value"]) is not int or not 1 <= row["value"] <= 10:
                raise ValueError("Ratings must be submitted integers from 1 to 10")
            order = (stamp, row["partition"], row["offset"])
            if key not in ratings or order > ratings[key][0]:
                ratings[key] = (order, row["value"])
    users = {user: {**row, "observed_units": 0, "covered_units": 0, "known_spend": Decimal(0),
                    "ratings": [], "missing_catalog_units": 0, "missing_fee_units": 0, "invalid_fee_units": 0}
             for user, row in cohort.items()}
    for (user, movie), stamp in watches.items():
        row = users[user]
        row["observed_units"] += 1
        if movie not in fees:
            row["missing_catalog_units"] += 1
        elif movie in invalid_fees:
            row["invalid_fee_units"] += 1
        elif fees[movie] is None:
            row["missing_fee_units"] += 1
        else:
            row["covered_units"] += 1
            row["known_spend"] += fees[movie]
        rating = ratings.get((user, movie))
        if rating is not None and rating[0][0] >= stamp:
            row["ratings"].append(rating[1])
    arms, spend_samples, rating_samples, complete_samples = {}, {}, {}, {}
    for arm in ("control", "treatment"):
        rows = [row for row in users.values() if row["arm"] == arm]
        units = sum(row["observed_units"] for row in rows)
        covered = sum(row["covered_units"] for row in rows)
        known = float(sum((row["known_spend"] for row in rows), Decimal(0)))
        rating_values = [value for row in rows for value in row["ratings"]]
        rating_samples[arm] = [float(np.mean(row["ratings"])) for row in rows if row["ratings"]]
        complete_samples[arm] = [float(row["known_spend"]) for row in rows if row["covered_units"] == row["observed_units"]]
        spend_samples[arm] = complete_samples[arm] if covered == units else []
        arms[arm] = {
            "assigned_users": len(rows), "users_with_watches": sum(bool(row["observed_units"]) for row in rows),
            "observed_units": units, "covered_units": covered, "fee_coverage": covered / units if units else None,
            "users_with_complete_fees": len(complete_samples[arm]),
            "known_spend_usd": known, "total_spend_usd": known if covered == units else None,
            "mean_spend_per_assigned_user_usd": known / len(rows) if rows and covered == units else None,
            "mean_known_spend_per_assigned_user_usd": known / len(rows) if rows else None,
            "mean_fee_per_observed_unit_usd": known / units if units and covered == units else None,
            "mean_known_fee_per_covered_unit_usd": known / covered if covered else None,
            **{key: sum(row[key] for row in rows) for key in ("missing_catalog_units", "missing_fee_units", "invalid_fee_units")},
            "rating_users": len(rating_samples[arm]), "rated_units": len(rating_values),
            "rating_user_coverage": len(rating_samples[arm]) / len(rows) if rows else None,
            "rating_unit_coverage": len(rating_values) / units if units else None,
            "mean_user_rating": float(np.mean(rating_samples[arm])) if rating_samples[arm] else None,
            "mean_submitted_rating": float(np.mean(rating_values)) if rating_values else None,
        }
    comparisons = {
        "spend_per_assigned_user_usd": welch(spend_samples["control"], spend_samples["treatment"]),
        "spend_per_user_with_complete_fees_usd": welch(complete_samples["control"], complete_samples["treatment"]),
        "mean_rating_per_rater": welch(rating_samples["control"], rating_samples["treatment"]),
    }
    if any(arm["covered_units"] != arm["observed_units"] for arm in arms.values()):
        comparisons["spend_per_assigned_user_usd"].update(status="unknown_missing_fees", difference=None, ci95=None, p_value=None)
    control_fee, treatment_fee = (arms[arm]["mean_fee_per_observed_unit_usd"] for arm in ("control", "treatment"))
    comparisons["mean_fee_per_observed_unit_usd"] = {
        "difference": treatment_fee - control_fee if control_fee is not None and treatment_fee is not None else None,
        "method": "descriptive pooled viewing-unit mean; no independent-view test",
    }
    return {"experiment_id": plan["id"], "arms": arms, "comparisons": comparisons, "telemetry": dict(counts),
            "has_users_in_both_arms": all(arm["assigned_users"] > 0 for arm in arms.values()),
            "assignment_balance_p_value": float(stats.binomtest(arms["treatment"]["assigned_users"], len(users), 0.5).pvalue) if users else None,
            "interpretation": "Assignment-based comparison, including failed requests, fallback responses and users with no watches. Ratings describe raters only; missing fees and ratings are unknown. Complete-fee users are a conditional secondary comparison. Tests assume independent users; 95% intervals are approximate and per metric, not joint. No sequential stopping on significance."}


def analyze_snapshot(directory):
    directory = Path(directory)
    snapshot = json.loads((directory / "snapshot.json").read_text())
    for name, digest in snapshot["files"].items():
        if sha256(directory / name) != digest:
            raise ValueError(f"Snapshot checksum mismatch: {name}")
    plan = json.loads((directory / "plan.json").read_text())
    fees, invalid = load_licensing_fees(directory / plan["fees_file"], fee_column=plan["fee_column"])
    result = analyze(plan, read_jsonl(directory / "requests.jsonl.gz"),
                     read_jsonl(directory / "outcomes.jsonl.gz"), fees, invalid)
    return {**result, "plan": plan, "capture": snapshot["capture"], "input_files": snapshot["files"]}


def markdown_report(result):
    def show(value):
        return "unknown" if value is None else f"{value:.6g}" if isinstance(value, float) else str(value)
    lines = [f"# Experiment: {result['experiment_id']}", "", "| Measure | Control | Treatment |", "| --- | ---: | ---: |"]
    for key in ("assigned_users", "observed_units", "fee_coverage", "mean_spend_per_assigned_user_usd",
                "mean_fee_per_observed_unit_usd", "rating_users", "rated_units", "rating_user_coverage", "mean_user_rating"):
        lines.append(f"| {key} | {show(result['arms']['control'][key])} | {show(result['arms']['treatment'][key])} |")
    lines += ["", "Differences are treatment minus control.", "", "| Comparison | Difference | 95% CI | p-value | Status |", "| --- | ---: | --- | ---: | --- |"]
    for key, comparison in result["comparisons"].items():
        interval = comparison.get("ci95")
        ci = "unknown" if interval is None else " to ".join(show(value) for value in interval)
        lines.append(f"| {key} | {show(comparison['difference'])} | {ci} | {show(comparison.get('p_value'))} | {comparison.get('status', 'descriptive')} |")
    lines += ["", result["interpretation"], "", "See results.json for fee/rating coverage, capture diagnostics, assignment balance and the fixed plan.", ""]
    return "\n".join(lines)
