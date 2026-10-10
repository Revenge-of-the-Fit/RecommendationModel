import base64
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from storage.database import WATCH_EVENTS, StorageError, open_database, watch_order
from storage.live import project_event
from storage.source_redaction import redact_bytes, redact_headers, redact_source_fields


@dataclass(frozen=True)
class KafkaEnvelope:
    source_id: str
    topic: str
    partition: int
    offset: int
    value: bytes | None
    key: bytes | None = None
    headers: list[tuple[str, bytes | None]] = field(default_factory=list)
    broker_timestamp_ms: int | None = None
    broker_timestamp_type: int = 0
    leader_epoch: int | None = None
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class EventStore:
    def __init__(self, path: Path, busy_timeout: float = 1.0):
        self.connection = open_database(path, busy_timeout)

    def save_event(self, envelope: KafkaEnvelope, parsed: dict) -> bool:
        if not envelope.source_id.strip() or not envelope.topic.strip():
            raise ValueError("Kafka source and topic are required")
        if envelope.partition < 0 or envelope.offset < 0:
            raise ValueError("Kafka partition and offset must be nonnegative")
        ingested_at = datetime.fromisoformat(envelope.ingested_at)
        if ingested_at.utcoffset() is None:
            raise ValueError("Ingestion timestamps must include a timezone")
        key, key_redacted = redact_bytes(envelope.key)
        value, value_redacted = redact_bytes(envelope.value)
        headers, headers_redacted = redact_headers(envelope.headers)
        encoded_headers = [
            [name, base64.b64encode(content).decode("ascii") if content is not None else None]
            for name, content in headers
        ]
        fingerprint_input = json.dumps({
            "key": base64.b64encode(key).decode("ascii") if key is not None else None,
            "value": base64.b64encode(value).decode("ascii") if value is not None else None,
            "headers": encoded_headers,
            "broker_timestamp_ms": envelope.broker_timestamp_ms,
            "broker_timestamp_type": envelope.broker_timestamp_type,
        }, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest()
        parsed = redact_source_fields(parsed)
        parsed_json = json.dumps(parsed, ensure_ascii=False, allow_nan=False, sort_keys=True)
        parameters = (
            envelope.source_id, envelope.topic, envelope.partition, envelope.offset,
            fingerprint, key, value, json.dumps(encoded_headers),
            int(key_redacted or value_redacted or headers_redacted),
            envelope.broker_timestamp_ms, envelope.broker_timestamp_type, envelope.leader_epoch,
            ingested_at.astimezone(timezone.utc).isoformat(), parsed.get("event_timestamp"), parsed.get("user_id"),
            parsed.get("movie_id"), parsed.get("event_type"), parsed["parse_status"],
            parsed["parser_version"], parsed_json,
        )
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            existing = self.connection.execute("""
                SELECT source_fingerprint FROM kafka_events
                WHERE source_id=? AND topic=? AND partition=? AND offset=?
            """, (envelope.source_id, envelope.topic, envelope.partition, envelope.offset)).fetchone()
            if existing is not None:
                if existing[0] != fingerprint:
                    raise StorageError("Kafka offset already contains a different source record")
                return False
            if (parsed.get("event_type") == "watch" and parsed["parse_status"] == "parsed"
                    and parsed.get("user_id") is not None and parsed.get("movie_id") is not None):
                identity = (envelope.source_id, envelope.topic, parsed["user_id"], parsed["movie_id"])
                previous = self.connection.execute(f"""
                    SELECT event_timestamp, broker_timestamp_ms, partition, offset FROM kafka_events
                    WHERE source_id=? AND topic=? AND user_id=? AND movie_id=? AND {WATCH_EVENTS}
                """, identity).fetchall()
                order = watch_order(parsed.get("event_timestamp"), envelope.broker_timestamp_ms,
                                    envelope.partition, envelope.offset)
                if previous and order <= max(watch_order(*row) for row in previous):
                    return False
                self.connection.execute(f"""
                    DELETE FROM kafka_events
                    WHERE source_id=? AND topic=? AND user_id=? AND movie_id=? AND {WATCH_EVENTS}
                """, identity)
            cursor = self.connection.execute("""
                INSERT INTO kafka_events (
                    source_id, topic, partition, offset, source_fingerprint, raw_key, raw_value,
                    headers_json, raw_redacted, broker_timestamp_ms, broker_timestamp_type,
                    leader_epoch, ingested_at, event_timestamp, user_id, movie_id, event_type,
                    parse_status, parser_version, parsed_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, topic, partition, offset) DO NOTHING
            """, parameters)
            inserted = cursor.rowcount == 1
            if inserted:
                self.connection.execute("""
                    INSERT INTO live_event_cursors(source_id, topic, partition) VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING
                """, (envelope.source_id, envelope.topic, envelope.partition))
                project_event(self.connection, envelope.source_id, envelope.topic, envelope.partition,
                              envelope.offset, parsed, envelope.broker_timestamp_ms, envelope.ingested_at)
        return inserted

    def get_event(self, source_id: str, topic: str, partition: int, offset: int) -> dict | None:
        cursor = self.connection.execute("""
            SELECT * FROM kafka_events WHERE source_id=? AND topic=? AND partition=? AND offset=?
        """, (source_id, topic, partition, offset))
        row = cursor.fetchone()
        return self._decode_row(cursor, row) if row else None

    def list_events(self, user_id: int, limit: int = 100) -> list[dict]:
        if not 1 <= limit <= 10000:
            raise ValueError("The event limit must be between 1 and 10000")
        cursor = self.connection.execute("""
            SELECT * FROM kafka_events WHERE user_id=?
            ORDER BY event_timestamp, source_id, topic, partition, offset LIMIT ?
        """, (user_id, limit))
        return [self._decode_row(cursor, row) for row in cursor]

    @staticmethod
    def _decode_row(cursor, row) -> dict:
        record = dict(zip((column[0] for column in cursor.description), row))
        record["parsed"] = json.loads(record.pop("parsed_json"))
        record["headers"] = [
            (name, base64.b64decode(value) if value is not None else None)
            for name, value in json.loads(record.pop("headers_json"))
        ]
        record["raw_redacted"] = bool(record["raw_redacted"])
        return record

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
