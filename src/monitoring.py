"""Collect operational metrics from bounded, read-only production observations."""
import argparse
import json
import os
import sqlite3
import time
from collections import Counter, defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from check_data_quality import check_recent_quality
from evaluate_online import load_licensing_fees, summarize_licensing, subsequent_hit, utc
from services.metadata import MetadataRateLimiter, split_rejected_batch
from services.metadata_http import MetadataHttpFetcher
from storage.event_payloads import parsed_event
from storage.payloads import load_json


SERVER = "cmu-online-mlip-vm02.andrew.cmu.edu:8082"
SOURCE = ("cmu-movielog", "movielog2")
PRICE_SOURCES = ["https://developers.openai.com/api/docs/guides/decisions#pricing-and-availability",
                 "https://developers.openai.com/api/docs/pricing"]
# Standard short-context USD / million input tokens, checked 2026-11-14.
DECISIONS_RATES = tuple(map(Decimal, ("0.10", "0.01", "0.125")))


def readonly(path):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    connection.row_factory = sqlite3.Row
    deadline = time.monotonic() + 30
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    return connection, deadline


def valid_slate(fields):
    # Use the same full top-20 eligibility rule as M2 online evaluation.
    items = fields.get("recommendations") or []
    if (not isinstance(items, list) or len(items) < 20 or not all(
        isinstance(item, dict) and type(item.get("rank")) is int
        and isinstance(item.get("movie_id"), str) and item["movie_id"] for item in items
    ) or sorted(item["rank"] for item in items) != list(range(1, len(items) + 1))):
        return None
    return tuple(item["movie_id"] for item in sorted(items, key=lambda item: item["rank"])[:20])


def collect_kafka(storage, *, recent_start, quality_start, quality_end, end, server, limit):
    connection, deadline = readonly(storage)
    with closing(connection):
        connection.execute("BEGIN")
        partitions = [row[0] for row in connection.execute(
            "SELECT partition FROM live_event_cursors WHERE source_id=? AND topic=? ORDER BY partition", SOURCE)]
        if not partitions or limit < len(partitions):
            raise ValueError("Missing Kafka partitions or row cap too small")
        statuses, impressions, viewing_units, watches = Counter(), set(), set(), defaultdict(set)
        coverage = {}
        unassigned = {"recent": 0, "quality": 0}
        bad_watches = 0
        for partition in partitions:
            earliest = latest = None
            count = 0
            quota = limit // len(partitions)
            rows = connection.execute("""
                SELECT event_timestamp,event_type,parse_status,user_id,movie_id,raw_value,parsed_json
                FROM kafka_events WHERE source_id=? AND topic=? AND partition=?
                ORDER BY offset DESC LIMIT ?
            """, (*SOURCE, partition, quota + 1))
            for row in rows:
                if time.monotonic() > deadline:
                    raise TimeoutError("Kafka read exceeded its 30-second budget")
                count += 1
                if count > quota:
                    break
                try:
                    stamp = utc(row["event_timestamp"])
                except ValueError:
                    continue
                earliest = min(earliest, stamp) if earliest else stamp
                latest = max(latest, stamp) if latest else stamp
                if stamp >= end:
                    continue
                kind = row["event_type"]
                recent, quality = recent_start <= stamp, quality_start <= stamp < quality_end
                if kind == "recommendation" and (recent or quality):
                    try:
                        fields = parsed_event(row)["fields"]
                        host = fields.get("server")
                    except (ValueError, KeyError, TypeError, AttributeError):
                        host = None
                    if not isinstance(host, str) or not host.strip():
                        unassigned["recent"] += int(recent)
                        unassigned["quality"] += int(quality)
                        continue
                    if host.strip().casefold() != server.casefold():
                        continue
                    status = fields.get("status")
                    if recent:
                        statuses[str(status) if type(status) is int else "unknown"] += 1
                    if quality and status == 200 and row["parse_status"] == "parsed" and (slate := valid_slate(fields)):
                        user = row["user_id"]
                        if type(user) is int and user > 0:
                            impressions.add((user, stamp, slate))
                elif kind == "watch" and stamp >= min(recent_start, quality_start) and row["parse_status"] == "parsed":
                    try:
                        minute = parsed_event(row)["fields"].get("minute")
                    except (ValueError, KeyError, TypeError, AttributeError):
                        bad_watches += 1
                        continue
                    user, movie = row["user_id"], row["movie_id"]
                    if type(user) is int and user > 0 and movie and type(minute) is int and minute >= 0:
                        if recent:
                            viewing_units.add((user, movie))
                        if stamp >= quality_start:
                            watches[(user, movie)].add(stamp)
            coverage[str(partition)] = {
                "oldest": earliest.isoformat() if earliest else None,
                "newest": latest.isoformat() if latest else None,
                "tail_capped": count > quota,
                "recent_bracketed": bool(earliest and earliest <= recent_start and latest >= end),
                "quality_bracketed": bool(earliest and earliest <= quality_start and latest >= end),
            }
    watch_index = {key: sorted(times) for key, times in watches.items()}
    follow_up = end - quality_end
    hits = sum(subsequent_hit(slate, user, stamp, watch_index, follow_up)
               for user, stamp, slate in impressions)
    total = sum(statuses.values())
    successes = statuses["200"]
    recent_covered = all(item["recent_bracketed"] for item in coverage.values())
    quality_covered = all(item["quality_bracketed"] for item in coverage.values())
    return {
        "coverage": coverage,
        "availability": {"requests": total, "successful": successes, "failed": total - successes,
                         "statuses": dict(statuses), "unattributed": unassigned["recent"],
                         "observed_ratio": successes / total if total else None,
                         "ratio": successes / total if recent_covered and total else None,
                         "conservative_ratio": (successes / (total + unassigned["recent"])
                                                if recent_covered and total else None)},
        "quality": {"impressions": len(impressions), "hits": hits, "unattributed": unassigned["quality"],
                    "unreadable_watches": bad_watches,
                    "hit_rate_at_20": (hits / len(impressions)
                                       if quality_covered and impressions and not bad_watches else None)},
        "licensing_covered": recent_covered and not bad_watches,
    }, viewing_units


def decisions_cost(record):
    """Unknown rates, usage or prompt-cache tiers stay unknown instead of zero."""
    usage = record.get("usage")
    model, endpoint = record.get("model"), record.get("endpoint")
    if (not isinstance(model, str) or not (model == "gpt-6-luna" or model.startswith("gpt-6-luna-"))
            or endpoint != "/v1/decisions" or not isinstance(usage, dict)):
        return None
    count = usage.get("input_tokens")
    if type(count) is not int or not 0 <= count <= 272000:
        return None
    details = usage.get("input_tokens_details")
    if not isinstance(details, dict):
        return None
    cached, written = details.get("cached_tokens"), details.get("cache_write_tokens")
    if any(type(n) is not int or n < 0 for n in (cached, written)) or cached + written > count:
        return None
    configuration = record.get("request") or {}
    retention = (configuration.get("prompt_cache_options") or {}).get("retention", "30m")
    if written and retention not in ("30m", "12h"):
        return None
    uncached_rate, cached_rate, write_rate = DECISIONS_RATES
    if retention == "12h":
        write_rate = Decimal("0.25")
    return ((count - cached - written) * uncached_rate + cached * cached_rate + written * write_rate) / 1_000_000


def collect_llm(storage, *, start, end, limit=10000):
    connection, deadline = readonly(storage)
    counts = Counter()
    known = Decimal(0)
    oldest = None
    with closing(connection):
        for row in connection.execute("SELECT started_at,record_json FROM llm_attempts ORDER BY rowid DESC LIMIT ?", (limit + 1,)):
            if time.monotonic() > deadline:
                raise TimeoutError("LLM read exceeded its 30-second budget")
            counts["rows_read"] += 1
            if counts["rows_read"] > limit:
                break
            stamp = utc(row["started_at"])
            oldest = min(oldest, stamp) if oldest else stamp
            if start <= stamp < end:
                counts["attempts"] += 1  # Primary key counts each attempt once, including retries.
                try:
                    cost = decisions_cost(load_json(row["record_json"]))
                except (ValueError, TypeError, AttributeError):
                    cost = None
                if cost is None:
                    counts["unknown_attempts"] += 1
                else:
                    counts["priced_attempts"] += 1
                    known += cost
    cap = counts["rows_read"] > limit
    return {"attempts": counts["attempts"], "priced_attempts": counts["priced_attempts"],
            "unknown_attempts": counts["unknown_attempts"], "tail_capped": cap,
            "known_cost_usd": float(known),
            "estimated_cost_usd": float(known) if not counts["unknown_attempts"] and (not cap or oldest < start) else None,
            "price_sources": PRICE_SOURCES, "price_checked": "2026-11-14"}


class MovieFeeCache:
    """Resolve catalog gaps with bounded, rate-limited metadata reads."""

    def __init__(self):
        self.entries = {}

    def collect(self, movies_by_count, metadata_url):
        now = time.monotonic()
        self.entries = {key: item for key, item in self.entries.items() if item[0] > now}
        missing = [movie for movie, _ in movies_by_count.most_common() if movie not in self.entries][:200]
        errors = Counter()
        if missing:
            limiter = MetadataRateLimiter(1)
            batches = deque(missing[index:index + 25] for index in range(0, len(missing), 25))
            with MetadataHttpFetcher(metadata_url, timeout=5) as client:
                for _ in range(12):
                    if not batches:
                        break
                    selected = batches.popleft()
                    limiter.wait()
                    batch = client("movie", selected)
                    if batch.error_type:
                        errors[str(batch.http_status) if batch.http_status else batch.error_type] += 1
                        if split := split_rejected_batch(batch, selected):
                            batches.extend(split)
                            continue
                        if len(selected) > 1:
                            continue  # Retry temporary batch failures in a later collection.
                    for movie in selected:
                        record = batch.records.get(movie, {})
                        raw = record.get("license_cost")
                        if raw is None:
                            raw = record.get("cost")
                        try:
                            fee = Decimal(str(raw))
                            if not fee.is_finite() or fee < 0:
                                fee = None
                        except InvalidOperation:
                            fee = None
                        ttl = 86400 if fee is not None else 3600 if batch.http_status in (400, 404) else 300
                        self.entries[movie] = (now + ttl, fee)
            if batches:
                errors["deferred_batches"] += len(batches)
        return {movie: self.entries[movie][1] for movie in movies_by_count
                if movie in self.entries and self.entries[movie][1] is not None}, dict(errors)


def collect_report(storage, catalog, reference, *, server=SERVER, limit=220000, clock=None,
                   lag_seconds=120, recent_minutes=10, quality_minutes=15, follow_up_seconds=3600,
                   metadata_url="http://128.2.24.239:8080", fees=None, fee_cache=None, skip_drift=False):
    now = clock or datetime.now(timezone.utc)
    end = now - timedelta(seconds=lag_seconds)
    recent_start = end - timedelta(minutes=recent_minutes)
    quality_end = end - timedelta(seconds=follow_up_seconds)
    quality_start = quality_end - timedelta(minutes=quality_minutes)
    report = {"generated_at": now.isoformat(), "server": server, "errors": {},
              "windows": {"recent_start": recent_start.isoformat(), "recent_end": end.isoformat(),
                          "quality_start": quality_start.isoformat(), "quality_end": quality_end.isoformat(),
                          "follow_up_seconds": follow_up_seconds}}
    errors = (OSError, ValueError, TypeError, KeyError, sqlite3.Error)
    try:
        kafka, units = collect_kafka(storage, recent_start=recent_start, quality_start=quality_start,
                                    quality_end=quality_end, end=end, server=server, limit=limit)
        report.update(kafka)
    except errors as error:
        report["errors"]["kafka"] = type(error).__name__
    else:
        try:
            prices, invalid = load_licensing_fees(catalog / "movies.csv.gz")
            if fees:
                saved_prices, _ = load_licensing_fees(fees, fee_column="license_cost_usd")
                prices.update({movie: fee for movie, fee in saved_prices.items() if fee is not None})
            counts = Counter(movie for _, movie in units if prices.get(movie) is None)
            missing = Counter(dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))))
            live_prices, lookup_errors = {}, {}
            if fee_cache is not None:
                try:
                    live_prices, lookup_errors = fee_cache.collect(missing, metadata_url)
                except errors as error:
                    report["errors"]["licensing_metadata"] = type(error).__name__
                prices.update(live_prices)
            cost = summarize_licensing(prices, invalid - {m for m, fee in prices.items() if fee is not None}, units)
            cost["metadata_resolved_movies"] = len(live_prices)
            cost["metadata_lookup_errors"] = lookup_errors
            if not kafka["licensing_covered"]:
                cost["total_cost_usd"] = None
            report["licensing"] = cost
        except errors as error:
            report["errors"]["licensing"] = type(error).__name__
    try:
        report["llm"] = collect_llm(storage, start=recent_start, end=end)
    except errors as error:
        report["errors"]["llm"] = type(error).__name__
    if not skip_drift:
        try:
            # Fee lookups can outlast the small drift tail. Anchor this window
            # at the drift read so newer arrivals do not crowd out every row.
            drift_end = clock or datetime.now(timezone.utc)
            report["drift"] = check_recent_quality(storage, catalog, reference,
                                                   start=drift_end - timedelta(minutes=recent_minutes), end=drift_end,
                                                   base_url=metadata_url)
        except errors as error:
            report["errors"]["drift"] = type(error).__name__
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--storage-path", type=Path, default=Path(os.getenv("STORAGE_PATH", "/app/state/events/events.sqlite3")))
    p.add_argument("--catalog-dir", type=Path, default=Path("/app/data"))
    p.add_argument("--reference", type=Path, default=Path(os.getenv("DATA_QUALITY_REFERENCE", "/app/models/data-quality-reference.json")))
    p.add_argument("--metadata-url", default=os.getenv("METADATA_BASE_URL", "http://128.2.24.239:8080"))
    p.add_argument("--server", default=os.getenv("MONITORING_SERVER", SERVER))
    p.add_argument("--event-limit", type=int, default=int(os.getenv("MONITORING_EVENT_LIMIT", "220000")))
    p.add_argument("--licensing-fees", type=Path)
    p.add_argument("--fetch-missing-fees", action="store_true")
    p.add_argument("--skip-drift", action="store_true", help="Skip metadata requests for a read-only sample")
    return p


def main():
    p = parser()
    args = p.parse_args()
    if args.event_limit <= 0 or not args.server.strip():
        p.error("Set a positive event cap and provide the exact course server field")
    report = collect_report(args.storage_path, args.catalog_dir, args.reference,
                            server=args.server.strip(), limit=args.event_limit, metadata_url=args.metadata_url,
                            fees=args.licensing_fees, fee_cache=MovieFeeCache() if args.fetch_missing_fees else None,
                            skip_drift=args.skip_drift)
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
