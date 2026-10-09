import logging
import math
import threading
import time
from pathlib import Path

from confluent_kafka import KafkaError, OFFSET_BEGINNING, TopicPartition

from events.parser import parse_event, resolve_timezone
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
    expected = TopicPartition(envelope.topic, envelope.partition, envelope.offset + 1)
    committed = consumer.commit(offsets=[expected], asynchronous=False)
    if not committed or len(committed) != 1:
        raise KafkaIngestionError("Offset commit did not return confirmation")
    result = committed[0]
    if result.error is not None:
        raise KafkaIngestionError("Offset commit failed")
    if (result.topic, result.partition, result.offset) != (expected.topic, expected.partition, expected.offset):
        raise KafkaIngestionError("Offset commit returned an unexpected position")


def run_consumer(
    consumer, store, source_id: str, topic: str = "movielog2", *,
    event_timezone: str | None = None, max_messages: int | None = None,
    idle_timeout: float | None = None, replay_from_start: bool = False,
    stop_event: threading.Event | None = None,
) -> dict:
    stats = {name: 0 for name in ("received", "stored", "duplicates", "parsed", "unrecognized", "failed", "committed")}
    stop_event = stop_event or threading.Event()

    def assign_from_start(client, partitions):
        client.assign([TopicPartition(item.topic, item.partition, OFFSET_BEGINNING) for item in partitions])

    try:
        if not source_id.strip() or not topic.strip():
            raise ValueError("A Kafka source and topic are required")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("The message limit must be positive")
        if idle_timeout is not None and (not math.isfinite(idle_timeout) or idle_timeout <= 0):
            raise ValueError("The idle timeout must be positive and finite")
        if event_timezone is not None:
            resolve_timezone(event_timezone)
        if replay_from_start:
            consumer.subscribe([topic], on_assign=assign_from_start)
        else:
            consumer.subscribe([topic])
        last_record = time.monotonic()
        while not stop_event.is_set() and (max_messages is None or stats["received"] < max_messages):
            message = consumer.poll(1.0)
            if message is None:
                if idle_timeout is not None and time.monotonic() - last_record >= idle_timeout:
                    break
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    if idle_timeout is not None and time.monotonic() - last_record >= idle_timeout:
                        break
                    continue
                raise KafkaIngestionError("Kafka poll returned an error")
            last_record = time.monotonic()
            envelope = capture_envelope(message, source_id)
            safe_value, _ = redact_bytes(envelope.value)
            parsed = parse_event(safe_value, event_timezone=event_timezone)
            inserted = store.save_event(envelope, parsed)
            stats["received"] += 1
            stats["stored" if inserted else "duplicates"] += 1
            stats[parsed["parse_status"]] += 1
            commit_envelope(consumer, envelope)
            stats["committed"] += 1
            if stats["received"] % 1000 == 0:
                LOGGER.info("Kafka ingestion progress: %s", stats)
        return stats
    finally:
        consumer.close()
