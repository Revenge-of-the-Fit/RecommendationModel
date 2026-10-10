import logging
import math
import threading
import time
from pathlib import Path

from confluent_kafka import KafkaError, OFFSET_BEGINNING, TopicPartition

from events.parser import parse_event, resolve_timezone
from events.watch_buffer import WatchBuffer
from storage.events import KafkaEnvelope
from storage.source_redaction import redact_bytes


LOGGER = logging.getLogger(__name__)


class KafkaIngestionError(RuntimeError):
    pass


class SafeKafkaLogger:
    def log(self, level, message, *args, **kwargs):
        LOGGER.log(level, "Kafka client diagnostic (level=%s)", level)


def kafka_error(error):
    LOGGER.error("Kafka client error (code=%s)", error.code())


def load_kafka_config(path: Path, group_id: str, offset_reset: str = "earliest") -> dict:
    if not group_id.strip():
        raise ValueError("A Kafka consumer group is required")
    if offset_reset not in ("earliest", "latest", "error"):
        raise ValueError("Invalid Kafka offset reset policy")
    config = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            raise ValueError("Invalid Kafka configuration line")
        config[key.strip()] = value.strip()
    config.setdefault("bootstrap.servers", "localhost:9092")
    config.update({
        "group.id": group_id,
        "group.protocol": "classic",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": offset_reset,
        "enable.partition.eof": False,
        "allow.auto.create.topics": False,
        "error_cb": kafka_error,
    })
    return config


def capture_envelope(message, source_id: str) -> KafkaEnvelope:
    timestamp_type, timestamp_ms = message.timestamp()
    epoch = getattr(message, "leader_epoch", None)
    return KafkaEnvelope(
        source_id=source_id, topic=message.topic(), partition=message.partition(),
        offset=message.offset(), value=message.value(), key=message.key(),
        headers=message.headers() or [], broker_timestamp_type=timestamp_type,
        broker_timestamp_ms=timestamp_ms if timestamp_ms is not None and timestamp_ms >= 0 else None,
        leader_epoch=epoch() if callable(epoch) else None,
    )


def commit_envelope(consumer, envelope: KafkaEnvelope) -> None:
    commit_offsets(consumer, {(envelope.topic, envelope.partition): envelope.offset + 1})


def commit_offsets(consumer, positions: dict) -> None:
    expected = {key: offset for key, offset in positions.items()}
    committed = consumer.commit(offsets=[
        TopicPartition(topic, partition, offset)
        for (topic, partition), offset in sorted(expected.items())
    ], asynchronous=False)
    if not committed or len(committed) != len(expected):
        raise KafkaIngestionError("Offset commit did not return confirmation")
    confirmed = {}
    for result in committed:
        if result.error is not None:
            raise KafkaIngestionError("Offset commit failed")
        key = (result.topic, result.partition)
        if key in confirmed or expected.get(key) != result.offset:
            raise KafkaIngestionError("Offset commit returned an unexpected position")
        confirmed[key] = result.offset


def run_consumer(
    consumer, store, source_id: str, topic: str = "movielog2", *,
    event_timezone: str | None = None, max_messages: int | None = None,
    idle_timeout: float | None = None, replay_from_start: bool = False,
    stop_event: threading.Event | None = None,
    batch_size: int = 500, batch_interval: float = 0.5,
    watch_idle_seconds: float = 300, max_watch_sessions: int = 50000,
) -> dict:
    stats = {name: 0 for name in ("received", "stored", "duplicates", "coalesced", "parsed", "unrecognized", "failed", "committed")}
    stop_event = stop_event or threading.Event()
    ready = []
    consumed = {}
    committed = {}
    timestamps = {}
    assigned = set()
    watches = None
    aborted = False

    def flush(now=None):
        if now is not None:
            watermark = min(timestamps.values()) if timestamps and assigned <= timestamps.keys() else None
            ready.extend(watches.expire(now, watermark))
        for start in range(0, len(ready), batch_size):
            inserted = store.save_events(ready[start:start + batch_size])
            stats["stored"] += sum(inserted)
            stats["duplicates"] += len(inserted) - sum(inserted)
        ready.clear()
        # A buffered watch pins the restart position until its latest observation is durable.
        blocked = watches.blocked_positions()
        targets = {key: blocked.get(key, position) for key, position in consumed.items()}
        targets = {key: position for key, position in targets.items()
                   if key not in committed or position[0] > committed[key][0]}
        if targets:
            commit_offsets(consumer, {key: position[0] for key, position in targets.items()})
            for key, position in targets.items():
                stats["committed"] += position[1] - committed.get(key, (0, 0))[1]
            committed.update(targets)
        stats["coalesced"] = watches.coalesced

    def on_assign(client, partitions):
        assigned.update((part.topic, part.partition) for part in partitions)
        if replay_from_start:
            client.assign([TopicPartition(item.topic, item.partition, OFFSET_BEGINNING) for item in partitions])

    def clear_assignment():
        ready.clear()
        watches.flush()
        consumed.clear()
        committed.clear()
        timestamps.clear()
        assigned.clear()

    def on_revoke(client, partitions):
        if not aborted:
            ready.extend(watches.flush())
            flush()
        clear_assignment()

    def on_lost(client, partitions):
        # Ownership has already changed; the next consumer replays the uncommitted records.
        clear_assignment()

    try:
        if not source_id.strip() or not topic.strip():
            raise ValueError("A Kafka source and topic are required")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("The message limit must be positive")
        if idle_timeout is not None and (not math.isfinite(idle_timeout) or idle_timeout <= 0):
            raise ValueError("The idle timeout must be positive and finite")
        if batch_size <= 0 or max_watch_sessions <= 0:
            raise ValueError("Batch and watch session limits must be positive")
        if any(not math.isfinite(value) or value <= 0 for value in (batch_interval, watch_idle_seconds)):
            raise ValueError("Batch and watch inactivity intervals must be positive and finite")
        if event_timezone is not None:
            resolve_timezone(event_timezone)
        watches = WatchBuffer(watch_idle_seconds, max_watch_sessions)
        consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
        last_record = time.monotonic()
        last_flush = last_record
        batch_count = 0
        while not stop_event.is_set() and (max_messages is None or stats["received"] < max_messages):
            message = consumer.poll(batch_interval)
            now = time.monotonic()
            if now - last_flush >= batch_interval or ready and message is None:
                flush(now)
                last_flush, batch_count = now, 0
            if message is None:
                if idle_timeout is not None and now - last_record >= idle_timeout:
                    break
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    if idle_timeout is not None and now - last_record >= idle_timeout:
                        break
                    continue
                raise KafkaIngestionError("Kafka poll returned an error")
            last_record = now
            envelope = capture_envelope(message, source_id)
            safe_value, _ = redact_bytes(envelope.value)
            parsed = parse_event(safe_value, event_timezone=event_timezone)
            key = (envelope.topic, envelope.partition)
            sequence = consumed.get(key, (0, 0))[1]
            ready.extend(watches.process(envelope, parsed, now, sequence))
            consumed[key] = (envelope.offset + 1, sequence + 1)
            if envelope.broker_timestamp_ms is not None:
                timestamps[key] = max(timestamps.get(key, envelope.broker_timestamp_ms), envelope.broker_timestamp_ms)
            stats["received"] += 1
            stats[parsed["parse_status"]] += 1
            batch_count += 1
            if batch_count >= batch_size:
                flush(now)
                last_flush, batch_count = now, 0
            if stats["received"] % 1000 == 0:
                LOGGER.info("Kafka ingestion progress: %s; active_watches=%s; processed_offsets=%s",
                            stats, len(watches), {f"{key[0]}:{key[1]}": value[0] for key, value in consumed.items()})
        ready.extend(watches.flush())
        flush()
        return stats
    except BaseException:
        aborted = True
        raise
    finally:
        consumer.close()
