"""Opt-in Kafka payload conversion, accounting, and bounded retention.

Only kafka_events is changed. Additive control tables have their own format
version so readers can be deployed before compression is explicitly activated.
"""
import hashlib
import json
import math
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from storage.database import StorageError, open_database
from storage.event_payloads import SENTINEL, pack_event, unpack_event

GIB = 1024**3
KEY = ("source_id", "topic", "partition", "offset")
COLUMNS = (*KEY, "source_fingerprint", "raw_key", "raw_value", "headers_json", "raw_redacted",
           "broker_timestamp_ms", "broker_timestamp_type", "leader_epoch", "ingested_at",
           "event_timestamp", "user_id", "movie_id", "event_type", "parse_status", "parser_version", "parsed_json")
DEFAULT_POLICY = {"history_bytes": 100 * GIB, "min_days": 10, "min_free_bytes": 16 * GIB,
                  "pins": [], "submission": None}


def utc(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("Retention timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def records(cursor):
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def batch_records(cursor):
    """Bound both row count (in SQL) and payload memory, allowing one large row."""
    names = [column[0] for column in cursor.description]
    rows, size = [], 0
    try:
        for values in cursor:
            row = dict(zip(names, values))
            rows.append(row)
            size += len(row["raw_value"] or b"") + len(row["parsed_json"].encode("utf-8"))
            if size >= 8 * 1024**2:
                break
    finally:
        cursor.close()
    return rows


def control(connection):
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='event_history_control' AND type='table'").fetchone():
        return None
    rows = records(connection.execute("SELECT * FROM event_history_control WHERE id=1"))
    if len(rows) != 1 or rows[0]["format_version"] != 1:
        raise StorageError("Unsupported event history control format")
    return rows[0]


def validate_policy(policy):
    if type(policy["history_bytes"]) is not int or policy["history_bytes"] <= 0:
        raise ValueError("History budget must be positive")
    if type(policy["min_days"]) is not int or policy["min_days"] < 10:
        raise ValueError("Keep at least ten recent UTC days")
    if type(policy["min_free_bytes"]) is not int or policy["min_free_bytes"] < 16 * GIB:
        raise ValueError("Keep at least 16 GiB of free filesystem space")
    if policy.get("submission") is not None:
        utc(policy["submission"])
    ids = set()
    for pin in policy["pins"]:
        if not pin["id"] or pin["id"] in ids or not pin["reason"] or utc(pin["start"]) >= utc(pin["end"]):
            raise ValueError("Pins need unique IDs, a reason, and an increasing time interval")
        ids.add(pin["id"])
    return policy


def activate(path, policy=None):
    policy = validate_policy(dict(DEFAULT_POLICY if policy is None else policy))
    connection = open_database(path, busy_timeout=2)
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            if control(connection) is not None:
                raise ValueError("Event history is already activated; use configure instead")
            connection.execute("""CREATE TABLE event_history_control (
                id INTEGER PRIMARY KEY CHECK(id=1), format_version INTEGER NOT NULL,
                policy_json TEXT NOT NULL, paused INTEGER NOT NULL DEFAULT 0,
                pruning_enabled INTEGER NOT NULL DEFAULT 0, activated_at TEXT NOT NULL,
                converted_rows INTEGER NOT NULL DEFAULT 0, expired_rows INTEGER NOT NULL DEFAULT 0,
                payload_bytes_saved INTEGER NOT NULL DEFAULT 0)""")
            connection.execute("""CREATE TABLE event_history_progress (
                source_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL,
                conversion_offset INTEGER NOT NULL, conversion_end INTEGER NOT NULL,
                retired_through INTEGER NOT NULL DEFAULT -1, last_run REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(source_id,topic,partition)) WITHOUT ROWID""")
            connection.execute("""CREATE TABLE event_history_days (
                day TEXT NOT NULL, source_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL,
                row_count INTEGER NOT NULL CHECK(row_count>=0), estimated_bytes INTEGER NOT NULL CHECK(estimated_bytes>=0),
                min_offset INTEGER NOT NULL, max_offset INTEGER NOT NULL,
                prune_offset INTEGER NOT NULL DEFAULT -1, prune_epoch TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(day,source_id,topic,partition)) WITHOUT ROWID""")
            for source, topic, partition in connection.execute("SELECT source_id,topic,partition FROM live_event_cursors").fetchall():
                latest = connection.execute("SELECT offset FROM kafka_events WHERE source_id=? AND topic=? AND partition=? ORDER BY offset DESC LIMIT 1",
                                            (source, topic, partition)).fetchone()
                connection.execute("INSERT INTO event_history_progress(source_id,topic,partition,conversion_offset,conversion_end) VALUES(?,?,?,-1,?)",
                                   (source, topic, partition, latest[0] if latest else -1))
            connection.execute("INSERT INTO event_history_control(id,format_version,policy_json,activated_at) VALUES(1,1,?,?)",
                               (json.dumps(policy), datetime.now(timezone.utc).isoformat()))
    finally:
        connection.close()


def configure(path, *, policy=None, paused=None, pruning=None):
    connection = open_database(path, busy_timeout=2)
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = control(connection)
            if previous is None:
                raise ValueError("Activate event history first")
            if policy is not None:
                connection.execute("UPDATE event_history_control SET policy_json=? WHERE id=1", (json.dumps(validate_policy(policy)),))
            for name, value in (("paused", paused), ("pruning_enabled", pruning)):
                if value is not None:
                    connection.execute(f"UPDATE event_history_control SET {name}=? WHERE id=1", (int(value),))
    finally:
        connection.close()


def retention_day(row):
    # Late arrivals receive ten days after ingestion as well as after the event.
    try:
        stamps = [utc(row["ingested_at"])]
        if row["event_timestamp"] is not None:
            stamps.append(utc(row["event_timestamp"]))
        if row["broker_timestamp_ms"] is not None:
            stamps.append(datetime.fromtimestamp(row["broker_timestamp_ms"] / 1000, timezone.utc))
        return max(stamps).date().isoformat()
    except (ValueError, TypeError, OverflowError, OSError, AttributeError):
        return "9999-12-31"  # Uncertain dates are never eligible for expiration.


def _varint(number):
    return 9 if number >= 2**56 else max(1, (number.bit_length() + 6) // 7)


def _record_size(values):
    body, header = 0, 1
    for value in values:
        if value is None:
            size, serial = 0, 0
        elif isinstance(value, int):
            size = next((size for size in (1, 2, 3, 4, 6, 8) if -(1 << (size * 8 - 1)) <= value < 1 << (size * 8 - 1)), 8)
            if value in (0, 1):
                size = 0
            serial = 1
        else:
            size = len(value.encode("utf-8") if isinstance(value, str) else value)
            serial = 13 + 2 * size
        body += size
        header += _varint(serial)
    return body + header + _varint(header) - 1


def estimate_bytes(row, page_size=4096):
    """SQLite index-btree payload/overflow estimate plus 50% local packing allowance.

    kafka_events is WITHOUT ROWID; secondary index entries include its PK.
    Full overflow pages are charged separately, never discounted as payload bytes.
    This is a conservative accounting estimate, not a physical table-size query.
    """
    entries = [[row[name] for name in COLUMNS],
               [row[name] for name in ("user_id", "event_timestamp", "event_type", *KEY)]]
    if (row["event_type"] == "watch" and row["parse_status"] == "parsed"
            and row["user_id"] is not None and row["movie_id"] is not None):
        entries.append([row[name] for name in ("source_id", "topic", "user_id", "movie_id", "partition", "offset")])
    total = 0
    minimum = (page_size - 12) * 32 // 255 - 23
    maximum = (page_size - 12) * 64 // 255 - 23
    for values in entries:
        payload = _record_size(values)
        local = payload
        if payload > maximum:
            local = minimum + (payload - minimum) % (page_size - 4)
            if local > maximum:
                local = minimum
        overflow = math.ceil((payload - local) / (page_size - 4))
        total += math.ceil((local + _varint(payload) + 2 + (4 if overflow else 0)) * 1.5) + overflow * page_size
    return total


def account(connection, row, direction, page_size=4096):
    if row["parsed_json"] != SENTINEL:
        return
    day = retention_day(row)
    key = (day, row["source_id"], row["topic"], row["partition"])
    size = estimate_bytes(row, page_size)
    if direction == 1:
        connection.execute("""INSERT INTO event_history_days
            (day,source_id,topic,partition,row_count,estimated_bytes,min_offset,max_offset)
            VALUES(?,?,?,?,1,?,?,?) ON CONFLICT(day,source_id,topic,partition) DO UPDATE SET
            row_count=row_count+1, estimated_bytes=estimated_bytes+excluded.estimated_bytes,
            min_offset=min(min_offset,excluded.min_offset), max_offset=max(max_offset,excluded.max_offset)
            """, (*key, size, row["offset"], row["offset"]))
        connection.execute("""INSERT INTO event_history_progress
            (source_id,topic,partition,conversion_offset,conversion_end) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING""",
                           (row["source_id"], row["topic"], row["partition"], row["offset"], row["offset"]))
    else:
        changed = connection.execute("""UPDATE event_history_days SET row_count=row_count-1,estimated_bytes=estimated_bytes-?
            WHERE day=? AND source_id=? AND topic=? AND partition=?""", (size, *key)).rowcount
        if changed != 1:
            raise StorageError("Missing event history accounting; transaction rolled back")


def expired_offset(connection, source, topic, partition, offset):
    floor = connection.execute("SELECT retired_through FROM event_history_progress WHERE source_id=? AND topic=? AND partition=?",
                               (source, topic, partition)).fetchone()
    return floor is not None and offset <= floor[0]


def conversion_batch(connection, limit=100):
    progress = records(connection.execute("SELECT * FROM event_history_progress WHERE conversion_offset<conversion_end ORDER BY last_run,source_id,topic,partition LIMIT 1"))
    if not progress:
        return {"scanned": 0, "converted": 0, "conversion_complete": True}
    progress = progress[0]
    key = tuple(progress[name] for name in KEY[:3])
    rows = batch_records(connection.execute("SELECT * FROM kafka_events WHERE source_id=? AND topic=? AND partition=? AND offset>? AND offset<=? ORDER BY offset LIMIT ?",
                                      (*key, progress["conversion_offset"], progress["conversion_end"], limit)))
    candidates = []
    before = after = 0
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    for row in rows:
        if row["parsed_json"] == SENTINEL:
            continue
        packed, sentinel = pack_event(row["raw_value"], row["parsed_json"])
        candidates.append((row, {**row, "raw_value": packed, "parsed_json": sentinel}))
    converted = 0
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        for previous, updated in candidates:
            changed = connection.execute("""UPDATE kafka_events SET raw_value=?,parsed_json=?
                WHERE source_id=? AND topic=? AND partition=? AND offset=? AND raw_value IS ? AND parsed_json=?""",
                                         (updated["raw_value"], SENTINEL, *(previous[name] for name in KEY), previous["raw_value"], previous["parsed_json"])).rowcount
            if changed:
                account(connection, updated, 1, page_size)
                converted += 1
                before += len(previous["raw_value"] or b"") + len(previous["parsed_json"].encode("utf-8"))
                after += len(updated["raw_value"]) + len(SENTINEL)
        end = rows[-1]["offset"] if rows else progress["conversion_end"]
        connection.execute("UPDATE event_history_progress SET conversion_offset=max(conversion_offset,?),last_run=? WHERE source_id=? AND topic=? AND partition=?",
                           (end, time.time(), *key))
        connection.execute("UPDATE event_history_control SET converted_rows=converted_rows+?,payload_bytes_saved=payload_bytes_saved+? WHERE id=1",
                           (converted, before - after))
    return {"scanned": len(rows), "converted": converted, "payload_bytes_before": before,
            "payload_bytes_after": after, "conversion_complete": False}


def protected_windows(policy, models):
    """Missing/malformed production metadata stops pruning instead of losing pins."""
    pins = list(policy["pins"])
    if policy.get("submission"):
        submission = utc(policy["submission"])
        pins.append({"id": "submission", "reason": "M3 grading window",
                     "start": (submission - timedelta(hours=72)).isoformat(),
                     "end": (submission + timedelta(hours=96)).isoformat()})
    models = Path(models)
    routing = json.loads((models / "routing.json").read_text())
    plans = []
    if routing.get("experiment"):
        plans.append(routing["experiment"])
    for path in (models / "experiments").glob("*/plan.json"):
        if not (path.parent / "snapshot" / "snapshot.json").is_file():
            plans.append(json.loads(path.read_text()))
    for plan in plans:
        pins.append({"id": "experiment:" + plan["id"], "reason": "Experiment awaiting completion/export",
                     "start": plan["start"], "end": plan["observation_end"]})
    for path in (models / "updates").glob("*/deployment.json"):
        deployment = json.loads(path.read_text())
        if deployment.get("status") != "deployed":
            continue
        evidence = path.parent / "availability.json"
        if evidence.is_file() and json.loads(evidence.read_text()).get("usable") is True:
            continue
        switch = utc(deployment["switched_at"])
        pins.append({"id": "update:" + path.parent.name, "reason": "Update awaiting Kafka availability evidence",
                     "start": (switch - timedelta(minutes=5)).isoformat(),
                     "end": (switch + timedelta(minutes=5)).isoformat()})
    for pin in pins:
        if utc(pin["start"]) >= utc(pin["end"]):
            raise ValueError("Invalid protected interval")
    return pins


def day_is_pinned(day, pins):
    # Round outward to UTC days, including the ending day for safe boundary evidence.
    return day == "9999-12-31" or any(utc(pin["start"]).date().isoformat() <= day <= utc(pin["end"]).date().isoformat() for pin in pins)


def row_is_pinned(row, pins):
    # A late arrival may have an ingestion day outside its pinned event day.
    try:
        days = [utc(row["ingested_at"]).date().isoformat()]
        if row["event_timestamp"]:
            days.append(utc(row["event_timestamp"]).date().isoformat())
        if row["broker_timestamp_ms"] is not None:
            days.append(datetime.fromtimestamp(row["broker_timestamp_ms"] / 1000, timezone.utc).date().isoformat())
        return any(day_is_pinned(day, pins) for day in days)
    except (ValueError, TypeError, OverflowError, OSError):
        return True


def history_status(connection, pins=(), now=None):
    current = control(connection)
    if current is None:
        return {"enabled": False}
    now = now or datetime.now(timezone.utc)
    policy = json.loads(current["policy_json"])
    days = records(connection.execute("SELECT day,sum(row_count) AS rows,sum(estimated_bytes) AS bytes FROM event_history_days WHERE row_count>0 GROUP BY day ORDER BY day"))
    protected = sum(day["bytes"] for day in days if day_is_pinned(day["day"], pins))
    total = sum(day["bytes"] for day in days)
    incomplete = connection.execute("SELECT count(*) FROM event_history_progress WHERE conversion_offset<conversion_end").fetchone()[0]
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    return {"enabled": True, "measured_at": now.isoformat(), "policy": policy,
            "paused": bool(current["paused"]), "pruning_enabled": bool(current["pruning_enabled"]),
            "conversion_complete": incomplete == 0, "converted_rows": current["converted_rows"],
            "expired_rows": current["expired_rows"], "payload_bytes_saved": current["payload_bytes_saved"],
            "accounted_rows": sum(day["rows"] for day in days), "estimated_kafka_bytes": total,
            "estimated_pinned_day_bytes": protected, "estimated_unpinned_bytes": total - protected,
            "over_target": total - protected > policy["history_bytes"],
            "oldest_accounted_day": days[0]["day"] if days else None, "accounted_days": len(days),
            "reusable_database_bytes": page_size * connection.execute("PRAGMA freelist_count").fetchone()[0],
            "pins": list(pins), "accounting": "Approximate Kafka table plus indexes; partial until conversion completes. Pinned rows within other days are conservatively included in the budget."}


def prune_batch(connection, policy, pins, *, now=None, limit=100):
    now = now or datetime.now(timezone.utc)
    current = control(connection)
    if not current or current["paused"] or not current["pruning_enabled"]:
        return {"state": "pruning_disabled", "expired": 0}
    status = history_status(connection, pins, now)
    if not status["conversion_complete"]:
        return {"state": "converting", "expired": 0}
    if not status["over_target"]:
        return {"state": "within_target", "expired": 0}
    cutoff = (now.date() - timedelta(days=policy["min_days"])).isoformat()
    # A new UTC day or changed protection policy reconsiders previously protected rows.
    epoch = hashlib.sha256(json.dumps([cutoff, policy, pins], sort_keys=True).encode()).hexdigest()
    buckets = records(connection.execute("SELECT * FROM event_history_days WHERE day<? AND row_count>0 ORDER BY day,source_id,topic,partition", (cutoff,)))
    bucket = next((item for item in buckets if not day_is_pinned(item["day"], pins)
                   and (item["prune_epoch"] != epoch or item["prune_offset"] < item["max_offset"])), None)
    if bucket is None:
        return {"state": "protected_or_scanned", "expired": 0}
    key = tuple(bucket[name] for name in KEY[:3])
    start = bucket["prune_offset"] if bucket["prune_epoch"] == epoch else bucket["min_offset"] - 1
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    expired = 0
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        # Policy may have been paused or changed since the maintenance pass began.
        latest = control(connection)
        if latest["paused"] or not latest["pruning_enabled"] or json.loads(latest["policy_json"]) != policy:
            return {"state": "policy_changed", "expired": 0}
        cursor = connection.execute("SELECT last_offset FROM live_event_cursors WHERE source_id=? AND topic=? AND partition=?", key).fetchone()
        projected = cursor[0] if cursor else -1
        rows = batch_records(connection.execute("SELECT * FROM kafka_events WHERE source_id=? AND topic=? AND partition=? AND offset>? AND offset<=? ORDER BY offset LIMIT ?",
                                          (*key, start, bucket["max_offset"], limit)))
        for row in rows:
            if (retention_day(row) != bucket["day"] or row["offset"] > projected or row_is_pinned(row, pins)):
                continue
            if row["parsed_json"] != SENTINEL:
                raise StorageError("Unaccounted legacy writer detected; pruning stopped")
            # Detect corruption before deleting a retained record.
            unpack_event(row["raw_value"], row["parsed_json"])
            connection.execute("DELETE FROM kafka_events WHERE source_id=? AND topic=? AND partition=? AND offset=?", (*key, row["offset"]))
            account(connection, row, -1, page_size)
            connection.execute("UPDATE event_history_progress SET retired_through=max(retired_through,?) WHERE source_id=? AND topic=? AND partition=?",
                               (row["offset"], *key))
            expired += 1
        end = rows[-1]["offset"] if rows else bucket["max_offset"]
        connection.execute("UPDATE event_history_days SET prune_offset=?,prune_epoch=? WHERE day=? AND source_id=? AND topic=? AND partition=?",
                           (end, epoch, bucket["day"], *key))
        connection.execute("UPDATE event_history_control SET expired_rows=expired_rows+? WHERE id=1", (expired,))
    return {"state": "pruning", "scanned": len(rows), "expired": expired, "day": bucket["day"]}


def maintain_batch(connection, path, models, *, limit=100):
    current = control(connection)
    if current is None or current["paused"]:
        return {"state": "paused" if current else "disabled"}
    policy = validate_policy(json.loads(current["policy_json"]))
    if shutil.disk_usage(Path(path).parent).free < policy["min_free_bytes"]:
        return {"state": "filesystem_reserve"}
    wal = Path(str(path) + "-wal")
    if wal.exists() and wal.stat().st_size > 256 * 1024**2:
        connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return {"state": "wal_backpressure"}
    result = conversion_batch(connection, limit)
    if not result["conversion_complete"]:
        return {"state": "converting", **result}
    if not current["pruning_enabled"]:
        return {"state": "pruning_disabled"}
    return prune_batch(connection, policy, protected_windows(policy, models), limit=limit)
