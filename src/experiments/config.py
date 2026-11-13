import math
import re
from datetime import timedelta

from evaluate_online import utc


def validate_plan(plan):
    if not isinstance(plan, dict) or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", plan.get("id", "")) is None:
        raise ValueError("Experiment ID must contain 1-64 letters, digits, hyphens or underscores")
    start, end = utc(plan["start"]), utc(plan["enrollment_end"])
    seconds = plan["follow_up_seconds"]
    if (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds) or seconds <= 0 or start >= end):
        raise ValueError("Choose increasing enrollment times and a positive follow-up")
    if utc(plan["observation_end"]) != end + timedelta(seconds=seconds):
        raise ValueError("Observation end must include every enrolled user's full follow-up")
    if plan["control_slot"] not in ("blue", "green") or plan["treatment_percent"] != 50:
        raise ValueError("Experiments use two slots and a fixed 50/50 split")
    if not isinstance(plan["salt"], str) or not plan["salt"]:
        raise ValueError("An experiment needs a saved random assignment salt")
    if set(plan["versions"]) != {"blue", "green"} or any(
        not isinstance(v, str) or not v for v in plan["versions"].values()
    ):
        raise ValueError("Record both served version identifiers before starting")
    if not plan["partitions"] or any(type(p) is not int or p < 0 for p in plan["partitions"]):
        raise ValueError("Declare the source partitions before starting")
    return plan


def active_plan(config, stamp):
    plan = config.get("experiment")
    return plan if plan and utc(plan["start"]) <= stamp < utc(plan["observation_end"]) else None


def arm_for_slot(plan, slot):
    return "control" if slot == plan["control_slot"] else "treatment"


def experiment_routing(plan):
    return {"primary": plan["control_slot"], "secondary_percent": plan["treatment_percent"], "salt": plan["salt"]}
