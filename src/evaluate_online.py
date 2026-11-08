"""Read-only subsequent-watch HitRate@20, licensing proxy, and recorded LLM tokens."""
import argparse
import csv
import gzip
import hashlib
import json
import math
import re
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


def utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Timestamps must be ISO strings with a timezone")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("Timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def has_subsequent_watch(times, stamp: datetime, follow_up: timedelta) -> bool:
    position = bisect_right(times, stamp)
    return position < len(times) and times[position] < stamp + follow_up


def subsequent_hit(movies: tuple[str, ...], user_id: int, stamp: datetime,
                   watches: dict, follow_up: timedelta) -> bool:
    return any(
        has_subsequent_watch(watches.get((user_id, movie_id), ()), stamp, follow_up)
        for movie_id in movies
    )


def licensing_cost(path: Path, viewing_units: set[tuple[int, str]]) -> dict:
    fees = {}
    invalid_fees = set()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if not {"movie_id", "license_cost"}.issubset(reader.fieldnames or []):
            raise ValueError("Catalog must contain movie_id and license_cost")
        for row in reader:
            movie_id = row["movie_id"]
            if not movie_id or movie_id in fees:
                raise ValueError("Catalog movie IDs must be nonempty and unique")
            raw_fee = row["license_cost"]
            fee = None
            if raw_fee and raw_fee.strip():
                try:
                    fee = Decimal(raw_fee)
                except InvalidOperation:
                    invalid_fees.add(movie_id)
                else:
                    if not fee.is_finite() or fee < 0:
                        invalid_fees.add(movie_id)
                        fee = None
            fees[movie_id] = fee
    counts = Counter({
        "observed_units": len(viewing_units), "covered_units": 0,
        "missing_catalog_units": 0, "missing_fee_units": 0, "invalid_fee_units": 0,
    })
    total = Decimal(0)
    for _, movie_id in sorted(viewing_units):
        if movie_id not in fees:
            counts["missing_catalog_units"] += 1
        elif movie_id in invalid_fees:
            counts["invalid_fee_units"] += 1
        elif fees[movie_id] is None:
            counts["missing_fee_units"] += 1
        else:
            counts["covered_units"] += 1
            total += fees[movie_id]
    return {
        "metric": "metadata licensing-cost proxy",
        "viewing_unit": "one distinct observed user/movie pair in the recommendation window",
        **counts,
        "fee_coverage": counts["covered_units"] / len(viewing_units) if viewing_units else None,
        "known_cost_usd": float(total),
        "total_cost_usd": float(total) if counts["covered_units"] == len(viewing_units) else None,
        "mean_known_fee_usd": float(total / counts["covered_units"]) if counts["covered_units"] else None,
    }


def recorded_llm_usage(path: Path, *, start: datetime, end: datetime, manifest: dict) -> dict:
    counts = Counter({
        "attempts": 0, "attempts_with_usage": 0, "missing_usage_attempts": 0,
        "invalid_usage_attempts": 0, "duplicate_export_rows": 0,
        "known_input_tokens": 0, "known_output_tokens": 0, "known_total_tokens": 0,
        "known_cached_input_tokens": 0, "known_cache_write_tokens": 0,
        "missing_cache_detail_attempts": 0,
    })
    statuses, models = Counter(), defaultdict(Counter)
    seen = {}
    exported_starts = []
    row_count = 0
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            row_count += 1
            row = json.loads(line)
            attempt_id = row["attempt_id"]
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                raise ValueError("LLM usage must identify each provider attempt")
            if attempt_id in seen:
                if seen[attempt_id] != row:
                    raise ValueError("Conflicting duplicate LLM attempt")
                counts["duplicate_export_rows"] += 1
                continue
            seen[attempt_id] = row
            stamp = utc(row["started_at"])
            if not utc(manifest["window_start_inclusive"]) <= stamp < utc(manifest["window_end_exclusive"]):
                raise ValueError("LLM attempt started outside the snapshot window")
            exported_starts.append(stamp)
            if not start <= stamp < end:
                continue
            status = row["status"]
            if status not in ("pending", "responded", "success", "failed"):
                raise ValueError("Unexpected LLM attempt status")
            statuses[status] += 1
            model = row.get("model") or "unknown"
            counts["attempts"] += 1
            models[model]["attempts"] += 1
            usage = row.get("usage")
            if usage is None:
                counts["missing_usage_attempts"] += 1
                models[model]["missing_usage_attempts"] += 1
                continue
            if not isinstance(usage, dict) or any(
                type(usage.get(key)) is not int or usage[key] < 0
                for key in ("input_tokens", "output_tokens")
            ):
                counts["invalid_usage_attempts"] += 1
                models[model]["invalid_usage_attempts"] += 1
                continue
            token_total = usage["input_tokens"] + usage["output_tokens"]
            if "total_tokens" in usage and (
                type(usage["total_tokens"]) is not int or usage["total_tokens"] != token_total
            ):
                counts["invalid_usage_attempts"] += 1
                models[model]["invalid_usage_attempts"] += 1
                continue
            for target in (counts, models[model]):
                target["attempts_with_usage"] += 1
                target["known_input_tokens"] += usage["input_tokens"]
                target["known_output_tokens"] += usage["output_tokens"]
                target["known_total_tokens"] += token_total
            details = usage.get("input_tokens_details")
            if not isinstance(details, dict) or any(
                type(details.get(key)) is not int or not 0 <= details[key] <= usage["input_tokens"]
                for key in ("cached_tokens", "cache_write_tokens")
            ):
                counts["missing_cache_detail_attempts"] += 1
                continue
            counts["known_cached_input_tokens"] += details["cached_tokens"]
            counts["known_cache_write_tokens"] += details["cache_write_tokens"]
    if row_count != manifest["counts"]["llm_attempts_exported"]:
        raise ValueError("LLM export row count does not match the manifest")
    return {
        "metric": "recorded provider token consumption",
        **counts,
        "total_tokens_for_exported_window_attempts": (
            counts["known_total_tokens"]
            if not counts["missing_usage_attempts"] and not counts["invalid_usage_attempts"] else None
        ),
        "status_counts": dict(sorted(statuses.items())),
        "by_model": {model: dict(sorted(values.items())) for model, values in sorted(models.items())},
        "export_tail_capped": manifest["llm_tail_capped"],
        "exported_attempt_start_min": min(exported_starts).isoformat() if exported_starts else None,
        "exported_attempt_start_max": max(exported_starts).isoformat() if exported_starts else None,
        "application_cache_reuses": None,
    }


def evaluate_snapshot(data_directory: Path, *, start: str, end: str,
                      observation_end: str, server: str,
                      follow_up_seconds: float = 3600) -> dict:
    start, end, observation_end = utc(start), utc(end), utc(observation_end)
    if not math.isfinite(follow_up_seconds) or follow_up_seconds <= 0:
        raise ValueError("Follow-up seconds must be positive and finite")
    follow_up = timedelta(seconds=follow_up_seconds)
    if follow_up <= timedelta(0) or start >= end or end + follow_up > observation_end:
        raise ValueError("Choose an increasing window with complete follow-up through end + follow-up")
    if not server.strip():
        raise ValueError("Provide the production recommendation server")

    data_directory = Path(data_directory)
    manifest_path = data_directory / "dataset-manifest.json"
    events_path = data_directory / "kafka_events.jsonl.gz"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    file_hashes = {}
    for name in ("kafka_events.jsonl.gz", "catalog.csv.gz", "llm_usage.jsonl.gz"):
        file_hashes[name] = sha256(data_directory / name)
        if file_hashes[name] != manifest["files"][name]:
            raise ValueError(f"{name} export checksum does not match the saved manifest")
    snapshot_start = utc(manifest["window_start_inclusive"])
    snapshot_end = utc(manifest["window_end_exclusive"])
    if not snapshot_start <= start < end <= observation_end <= snapshot_end:
        raise ValueError("Evaluation and follow-up must be inside the snapshot window")
    model_hash = manifest["reference_model_sha256"]
    if not isinstance(model_hash, str) or re.fullmatch(r"[0-9a-f]{64}", model_hash) is None:
        raise ValueError("Snapshot must identify its reference model SHA-256")
    partitions = manifest["partitions"]
    if not partitions:
        raise ValueError("Snapshot must identify its captured partitions")

    counts = Counter()
    bounds = {}
    seen_offsets = {}
    impressions = set()
    watches = defaultdict(set)
    viewing_units = set()
    with gzip.open(events_path, "rt", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            counts["source_rows"] += 1
            row = json.loads(line)
            if row["source_id"] != manifest["source_id"] or row["topic"] != manifest["topic"]:
                raise ValueError(f"Unexpected source/topic at export line {line_number}")
            partition, offset = row["partition"], row["offset"]
            if type(partition) is not int or type(offset) is not int or offset < 0:
                raise ValueError(f"Invalid source coordinates at export line {line_number}")
            if str(partition) not in partitions:
                raise ValueError(f"Undeclared partition at export line {line_number}")
            identity = (partition, offset)
            fingerprint = hashlib.sha256(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).digest()
            if identity in seen_offsets:
                if seen_offsets[identity] != fingerprint:
                    raise ValueError(f"Conflicting duplicate source coordinates at line {line_number}")
                counts["duplicate_source_rows"] += 1
                continue
            seen_offsets[identity] = fingerprint
            if not partitions[str(partition)]["lowest_offset"] <= offset <= partitions[str(partition)]["highest_offset"]:
                raise ValueError(f"Source offset outside manifest bounds at line {line_number}")
            try:
                stamp, ingested = utc(row["event_timestamp"]), utc(row["ingested_at"])
            except ValueError:
                counts["invalid_timestamps"] += 1
                continue
            if not snapshot_start <= stamp < snapshot_end or ingested > snapshot_end:
                raise ValueError(f"Event outside snapshot time/as-of bounds at line {line_number}")
            low, high = bounds.get(partition, (stamp, stamp))
            bounds[partition] = (min(low, stamp), max(high, stamp))
            if row["parse_status"] != "parsed":
                counts["unparsed_rows"] += 1
                continue
            user_id = row["user_id"]
            if type(user_id) is not int or user_id <= 0:
                counts["invalid_user_rows"] += 1
                continue
            if row["event_type"] == "watch" and start <= stamp < observation_end:
                movie_id = row["movie_id"]
                minute = row["parsed"]["fields"].get("minute")
                if not isinstance(movie_id, str) or not movie_id or type(minute) is not int or minute < 0:
                    counts["invalid_watch_rows"] += 1
                    continue
                times = watches[(user_id, movie_id)]
                counts["duplicate_watch_observations"] += int(stamp in times)
                times.add(stamp)
                counts["watch_rows_in_observation_window"] += 1
                if stamp < end:
                    viewing_units.add((user_id, movie_id))
            elif row["event_type"] == "recommendation" and start <= stamp < end:
                counts["recommendation_rows_in_request_window"] += 1
                fields = row["parsed"]["fields"]
                if fields.get("server") != server:
                    counts["other_server_recommendations"] += 1
                    continue
                if fields.get("status") != 200:
                    counts["unsuccessful_recommendations"] += 1
                    continue
                slate = fields.get("recommendations", [])
                if not isinstance(slate, list) or not all(
                    isinstance(item, dict) and type(item.get("rank")) is int
                    and isinstance(item.get("movie_id"), str) and item["movie_id"]
                    for item in slate
                ):
                    counts["invalid_recommendation_slates"] += 1
                    continue
                ranks = [item["rank"] for item in slate]
                if sorted(ranks) != list(range(1, len(slate) + 1)):
                    counts["invalid_recommendation_slates"] += 1
                    continue
                if len(slate) < 20:
                    counts["short_recommendation_slates"] += 1
                    continue
                movies = tuple(item["movie_id"] for item in sorted(slate, key=lambda item: item["rank"])[:20])
                impression = (user_id, stamp, movies)
                counts["duplicate_impressions"] += int(impression in impressions)
                if impression not in impressions:
                    counts["duplicate_top20_movie_slots"] += 20 - len(set(movies))
                    impressions.add(impression)

    if counts["source_rows"] != manifest["counts"]["source_events_exported"]:
        raise ValueError("Export row count does not match the manifest")
    for partition in partitions:
        if int(partition) not in bounds:
            raise ValueError(f"No timestamped observations for partition {partition}")
        low, high = bounds[int(partition)]
        if low > start or high < observation_end:
            raise ValueError(f"Partition {partition} does not span the requested window and follow-up")
    if not impressions:
        raise ValueError("No eligible production recommendations in the requested window")

    watch_index = {pair: sorted(times) for pair, times in watches.items()}
    user_watches = defaultdict(set)
    for (user_id, _), times in watches.items():
        user_watches[user_id].update(times)
    user_watch_index = {user_id: sorted(times) for user_id, times in user_watches.items()}
    observed_watchers = sum(
        has_subsequent_watch(user_watch_index.get(user_id, ()), stamp, follow_up)
        for user_id, stamp, _ in impressions
    )
    hits = sum(
        subsequent_hit(movies, user_id, stamp, watch_index, follow_up)
        for user_id, stamp, movies in impressions
    )
    licensing = licensing_cost(data_directory / "catalog.csv.gz", viewing_units)
    llm_usage = recorded_llm_usage(data_directory / "llm_usage.jsonl.gz", start=start, end=end, manifest=manifest)
    return {
        "metric": "subsequent-watch HitRate@20 (retained observations)",
        "implementation_sha256": sha256(Path(__file__)),
        "top_k": 20,
        "eligible_recommendations": len(impressions),
        "recommendations_with_subsequent_watch": hits,
        "hit_rate_at_20": hits / len(impressions),
        "conditional_hit_rate_at_20": hits / observed_watchers if observed_watchers else None,
        "recommendations_with_any_observed_watch": observed_watchers,
        "recommendations_without_observed_watch": len(impressions) - observed_watchers,
        "recommendations_with_observed_watch_but_no_top20_match": observed_watchers - hits,
        "observed_watch_rate": observed_watchers / len(impressions),
        "no_observed_watch_rate": (len(impressions) - observed_watchers) / len(impressions),
        "unique_recommended_users": len({user_id for user_id, _, _ in impressions}),
        "window": {
            "recommendations_start_inclusive": start.isoformat(),
            "recommendations_end_exclusive": end.isoformat(),
            "observation_end_exclusive": observation_end.isoformat(),
            "follow_up_seconds": follow_up_seconds,
            "matching_interval": "recommendation_time < watch_time < recommendation_time + follow_up",
        },
        "snapshot": {
            "manifest_sha256": sha256(manifest_path),
            "kafka_events_sha256": file_hashes["kafka_events.jsonl.gz"],
            "files_sha256": file_hashes,
            "source_id": manifest["source_id"],
            "topic": manifest["topic"],
            "window_start_inclusive": snapshot_start.isoformat(),
            "window_end_exclusive": snapshot_end.isoformat(),
            "partitions": {
                key: {
                    **value,
                    "observed_start": bounds[int(key)][0].isoformat(),
                    "observed_end": bounds[int(key)][1].isoformat(),
                }
                for key, value in partitions.items()
            },
            "limits_note": manifest.get("limits_note"),
        },
        "model": {
            "reference_model_sha256": model_hash,
            "provenance": "Pre-training reference from the snapshot manifest; not a per-response serving fingerprint.",
        },
        "server": server,
        "counts": dict(sorted(counts.items())),
        "cost": {
            "window_start_inclusive": start.isoformat(),
            "window_end_exclusive": end.isoformat(),
            "movie_licensing_usd": licensing["total_cost_usd"],
            "movie_licensing_known_usd": licensing["known_cost_usd"],
            "llm_tokens": llm_usage["total_tokens_for_exported_window_attempts"],
            "llm_usd": None,
            "licensing": licensing,
            "llm_usage": llm_usage,
        },
        "limitations": [
            "Each eligible response counts once, including responses without any observed later watch.",
            "Conditional HitRate@20 uses only responses with any observed watch in the same follow-up interval; no such responses yields null.",
            "Conditioning selects an observed-watching cohort and changes the denominator; it does not replace the all-response metric.",
            "No observed watch is not proof of inactivity because retained observations may omit watching.",
            "Movie IDs match exactly; ranks 1-20 are used without refilling duplicate movie slots.",
            "Source coordinates are deduplicated; identical user/time/ranked-slate impressions count once.",
            "Watch timestamps are deduplicated per user/movie; repeated chunks cannot increase a response's binary hit.",
            "One watch can match multiple prior responses; subsequent watching does not establish causal attribution.",
            "Full follow-up duration is inside every partition's observed time span; this does not prove complete event coverage.",
            "Bounded tails and latest-position retention can omit watches and recommendations; this is not all-traffic HitRate.",
            "A later watch position may continue an earlier viewing; repeated views, starts and completions cannot be reconstructed.",
            "Kafka impressions lack per-response model hashes; the manifest reference is not proof of the model served.",
            "Licensing uses all observed user/movie pairs in the request window, not only recommendation matches.",
            "Each observed user/movie pair incurs one metadata fee; minute chunks and repeated views are not separate units.",
            "Licensing is a sampled proxy, not an invoice; retained history can miss pairs and collapse repeated viewings.",
            "Missing or invalid metadata fees are unknown, not free; only the covered-unit subtotal is known.",
            "LLM attempts are deduplicated by attempt ID; distinct retries count separately, including failed attempts with usage.",
            "Profile cache reuse is not a new provider attempt; its frequency cannot be recovered from this usage export.",
            "Cached-input and cache-write tokens are subsets of input, not additional tokens.",
            "LLM totals describe recorded attempts started in the request window, not an uncapped billing total.",
            "Missing/invalid usage is unknown consumption; cache-detail gaps do not imply zero cached tokens.",
            "LLM USD is calculated manually from provider usage in m2_report.md; this evaluator leaves it null, not zero.",
        ],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--start", required=True, help="Inclusive recommendation-window start, with timezone")
    parser.add_argument("--end", required=True, help="Exclusive recommendation-window end, with timezone")
    parser.add_argument("--observation-end", required=True, help="Exclusive observed-event cutoff, with timezone")
    parser.add_argument("--follow-up-seconds", type=float, default=3600)
    parser.add_argument("--server", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.output.resolve().is_relative_to(args.data_dir.resolve()):
            raise ValueError("Output must be outside the read-only snapshot directory")
        result = evaluate_snapshot(
            args.data_dir, start=args.start, end=args.end, observation_end=args.observation_end,
            follow_up_seconds=args.follow_up_seconds, server=args.server,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    except (ValueError, KeyError, TypeError, OSError, OverflowError) as error:
        parser.exit(1, f"Online evaluation failed ({type(error).__name__}): {error}\n")
    print(json.dumps({
        "eligible_recommendations": result["eligible_recommendations"],
        "recommendations_with_subsequent_watch": result["recommendations_with_subsequent_watch"],
        "hit_rate_at_20": result["hit_rate_at_20"],
        "conditional_hit_rate_at_20": result["conditional_hit_rate_at_20"],
        "recommendations_with_any_observed_watch": result["recommendations_with_any_observed_watch"],
        "recommendations_without_observed_watch": result["recommendations_without_observed_watch"],
        "recommendations_with_observed_watch_but_no_top20_match": result["recommendations_with_observed_watch_but_no_top20_match"],
        "observed_watch_rate": result["observed_watch_rate"],
        "no_observed_watch_rate": result["no_observed_watch_rate"],
        "movie_licensing_usd": result["cost"]["movie_licensing_usd"],
        "movie_licensing_known_usd": result["cost"]["movie_licensing_known_usd"],
        "licensing_fee_coverage": result["cost"]["licensing"]["fee_coverage"],
        "llm_tokens": result["cost"]["llm_tokens"],
        "llm_usd": result["cost"]["llm_usd"],
        "output": str(args.output.resolve()),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
