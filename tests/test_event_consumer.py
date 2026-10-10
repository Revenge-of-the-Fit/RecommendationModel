import logging
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka import KafkaError, OFFSET_BEGINNING, TopicPartition

from events.consumer import (
    KafkaIngestionError, SafeKafkaLogger, capture_envelope, commit_envelope,
    kafka_error, load_kafka_config, run_consumer,
)
from storage.events import EventStore


class FakeMessage:
    def __init__(self, offset=100, value=None, error=None, timestamp=(1, 1791460800000)):
        self.position = offset
        self.content = value if value is not None else b"2026-10-08T12:00:00+00:00,42,GET /data/m/movie_a/17.mpg"
        self.kafka_error = error
        self.broker_timestamp = timestamp

    def topic(self):
        return "movielog2"

    def partition(self):
        return 2

    def offset(self):
        return self.position

    def value(self):
        return self.content

    def key(self):
        return b"user-42"

    def headers(self):
        return [("trace-id", b"trace-1")]

    def timestamp(self):
        return self.broker_timestamp

    def leader_epoch(self):
        return 3

    def error(self):
        return self.kafka_error


class FakeConsumer:
    def __init__(self, messages=(), on_commit=None, assigned_partitions=()):
        self.messages = list(messages)
        self.on_commit = on_commit
        self.assigned_partitions = list(assigned_partitions)
        self.subscriptions = []
        self.assignments = []
        self.commits = []
        self.polls = []
        self.closed = 0

    def subscribe(self, topics, **kwargs):
        self.subscriptions.append((topics, kwargs))
        if "on_assign" in kwargs:
            kwargs["on_assign"](self, self.assigned_partitions)

    def assign(self, partitions):
        self.assignments.append(partitions)

    def poll(self, timeout):
        self.polls.append(timeout)
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, offsets, asynchronous):
        self.commits.append((offsets, asynchronous))
        if self.on_commit:
            return self.on_commit(offsets)
        return [SimpleNamespace(topic=part.topic, partition=part.partition, offset=part.offset, error=None) for part in offsets]

    def close(self):
        self.closed += 1


class KafkaConsumerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def test_committed_offset_follows_durable_record_and_is_next_position(self):
        def inspect_commit(offsets):
            with EventStore(self.path) as reader:
                saved = reader.get_event("course-cluster", "movielog2", 2, 100)
                self.assertIsNotNone(saved)
                self.assertEqual(saved["raw_value"], message.value())
                self.assertEqual(saved["parsed"]["fields"]["minute"], 17)
            return [SimpleNamespace(topic=offsets[0].topic, partition=offsets[0].partition, offset=offsets[0].offset, error=None)]

        message = FakeMessage()
        consumer = FakeConsumer([message], on_commit=inspect_commit)
        with EventStore(self.path) as store:
            stats = run_consumer(consumer, store, "course-cluster", max_messages=1)
        self.assertEqual(stats, {
            "received": 1, "stored": 1, "duplicates": 0, "parsed": 1,
            "unrecognized": 0, "failed": 0, "committed": 1, "coalesced": 0,
        })
        offsets, asynchronous = consumer.commits[0]
        self.assertFalse(asynchronous)
        self.assertEqual((offsets[0].topic, offsets[0].partition, offsets[0].offset), ("movielog2", 2, 101))
        self.assertEqual(consumer.closed, 1)
        self.assertEqual(consumer.subscriptions[0][0], ["movielog2"])
        self.assertEqual(set(consumer.subscriptions[0][1]), {"on_assign", "on_revoke", "on_lost"})

    def test_store_failure_never_commits_and_closes_consumer(self):
        consumer = FakeConsumer([FakeMessage()])
        store = SimpleNamespace(save_events=Mock(side_effect=sqlite3.OperationalError("test disk failure")))
        with self.assertRaises(sqlite3.OperationalError):
            run_consumer(consumer, store, "course-cluster", max_messages=1)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.closed, 1)

    def test_failed_commit_preserves_record_and_replay_is_duplicate(self):
        def failed_commit(_):
            raise RuntimeError("simulated connection loss")

        first = FakeConsumer([FakeMessage()], on_commit=failed_commit)
        with EventStore(self.path) as store:
            with self.assertRaises(RuntimeError):
                run_consumer(first, store, "course-cluster", max_messages=1)
            original = store.get_event("course-cluster", "movielog2", 2, 100)
            self.assertIsNotNone(original)
        replay = FakeConsumer([FakeMessage()])
        with EventStore(self.path) as store:
            stats = run_consumer(replay, store, "course-cluster", max_messages=1)
            self.assertEqual(store.get_event("course-cluster", "movielog2", 2, 100), original)
        self.assertEqual(stats["stored"], 0)
        self.assertEqual(stats["duplicates"], 1)
        self.assertEqual(stats["committed"], 1)
        self.assertEqual(first.closed, 1)
        self.assertEqual(replay.closed, 1)

    def test_commit_partition_error_attribute_prevents_acknowledgement(self):
        envelope = capture_envelope(FakeMessage(), "course-cluster")
        consumer = FakeConsumer(on_commit=lambda offsets: [SimpleNamespace(
            topic=offsets[0].topic, partition=offsets[0].partition,
            offset=offsets[0].offset, error=KafkaError(KafkaError._TRANSPORT),
        )])
        with self.assertRaisesRegex(KafkaIngestionError, "Offset commit failed"):
            commit_envelope(consumer, envelope)

    def test_commit_requires_exact_single_partition_confirmation(self):
        envelope = capture_envelope(FakeMessage(), "course-cluster")
        replies = (
            None,
            [],
            [SimpleNamespace(topic="different", partition=2, offset=101, error=None)],
            [SimpleNamespace(topic="movielog2", partition=3, offset=101, error=None)],
            [SimpleNamespace(topic="movielog2", partition=2, offset=100, error=None)],
            [
                SimpleNamespace(topic="movielog2", partition=2, offset=101, error=None),
                SimpleNamespace(topic="movielog2", partition=3, offset=101, error=None),
            ],
        )
        for reply in replies:
            with self.subTest(reply=reply):
                consumer = FakeConsumer(on_commit=lambda _, reply=reply: reply)
                with self.assertRaises(KafkaIngestionError):
                    commit_envelope(consumer, envelope)

    def test_namespace_separates_same_offsets_across_clusters(self):
        with EventStore(self.path) as store:
            for source in ("cluster-one", "cluster-two"):
                stats = run_consumer(FakeConsumer([FakeMessage()]), store, source, max_messages=1)
                self.assertEqual(stats["stored"], 1)
                saved = store.get_event(source, "movielog2", 2, 100)
                self.assertEqual(saved["source_id"], source)
            self.assertEqual(len(store.list_events(42)), 2)

    def test_failed_unknown_and_tombstone_records_are_saved_and_committed(self):
        tombstone = FakeMessage(offset=103)
        tombstone.content = None
        messages = [
            FakeMessage(offset=100),
            FakeMessage(offset=101, value=b"2026-10-08T12:00:00+00:00,42,GET /future/action"),
            FakeMessage(offset=102, value=b"\xff"),
            tombstone,
        ]
        consumer = FakeConsumer(messages)
        with EventStore(self.path) as store:
            stats = run_consumer(consumer, store, "course-cluster", max_messages=4)
            for message in messages:
                saved = store.get_event("course-cluster", message.topic(), message.partition(), message.offset())
                self.assertEqual(saved["raw_value"], message.value())
        self.assertEqual(stats["parsed"], 1)
        self.assertEqual(stats["unrecognized"], 1)
        self.assertEqual(stats["failed"], 2)
        self.assertEqual(stats["stored"], 4)
        self.assertEqual(stats["committed"], 4)

    def test_deep_opaque_json_is_saved_and_committed_without_recursion_failure(self):
        value = b"[" * 2000 + b"0" + b"]" * 2000
        consumer = FakeConsumer([FakeMessage(value=value)])
        with EventStore(self.path) as store:
            stats = run_consumer(consumer, store, "course-cluster", max_messages=1)
            saved = store.get_event("course-cluster", "movielog2", 2, 100)
            self.assertEqual(saved["raw_value"], value)
            self.assertEqual(saved["parsed"]["parse_status"], "failed")
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["committed"], 1)
        self.assertEqual(consumer.closed, 1)

    def test_eof_is_ignored_and_poll_errors_close_consumer(self):
        consumer = FakeConsumer([
            FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF)), FakeMessage(),
        ])
        with EventStore(self.path) as store:
            stats = run_consumer(consumer, store, "course-cluster", max_messages=1)
        self.assertEqual(stats["received"], 1)
        self.assertEqual(len(consumer.commits), 1)
        failed = FakeConsumer([FakeMessage(error=KafkaError(KafkaError._ALL_BROKERS_DOWN))])
        with EventStore(self.path) as store:
            with self.assertRaises(KafkaIngestionError):
                run_consumer(failed, store, "course-cluster", max_messages=1)
        self.assertEqual(failed.commits, [])
        self.assertEqual(failed.closed, 1)

    def test_idle_limit_and_stop_event_end_polling_and_close(self):
        consumer = FakeConsumer([None])
        with EventStore(self.path) as store:
            with patch("events.consumer.time.monotonic", side_effect=(0, 2)):
                stats = run_consumer(consumer, store, "course-cluster", idle_timeout=1)
        self.assertEqual(stats["received"], 0)
        self.assertEqual(consumer.polls, [0.5])
        self.assertEqual(consumer.closed, 1)
        stopped = threading.Event()
        stopped.set()
        consumer = FakeConsumer([FakeMessage()])
        with EventStore(self.path) as store:
            stats = run_consumer(consumer, store, "course-cluster", stop_event=stopped)
        self.assertEqual(stats["received"], 0)
        self.assertEqual(consumer.polls, [])
        self.assertEqual(consumer.closed, 1)

    def test_repeated_eof_messages_do_not_extend_idle_deadline(self):
        consumer = FakeConsumer([
            FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF)),
            FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF)),
        ])
        with patch("events.consumer.time.monotonic", side_effect=(0, 2, 3)):
            stats = run_consumer(consumer, Mock(), "course-cluster", idle_timeout=1)
        self.assertEqual(stats["received"], 0)
        self.assertEqual(consumer.polls, [0.5])
        self.assertEqual(consumer.closed, 1)

    def test_invalid_bounds_and_configuration_close_before_subscription(self):
        cases = (
            {"source_id": ""}, {"topic": " "}, {"max_messages": 0},
            {"max_messages": -1}, {"idle_timeout": 0},
            {"idle_timeout": float("inf")}, {"idle_timeout": float("nan")},
            {"event_timezone": "+24:00"},
            {"batch_size": 0}, {"batch_interval": float("nan")},
            {"watch_idle_seconds": 0}, {"max_watch_sessions": 0},
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                consumer = FakeConsumer()
                with self.assertRaises(ValueError):
                    run_consumer(consumer, Mock(), **{"source_id": "course-cluster", **arguments})
                self.assertEqual(consumer.subscriptions, [])
                self.assertEqual(consumer.closed, 1)

    def test_replay_assignment_starts_every_partition_from_retained_beginning(self):
        stopped = threading.Event()
        stopped.set()
        consumer = FakeConsumer(assigned_partitions=[
            TopicPartition("movielog2", 0, 900), TopicPartition("movielog2", 1, 1200),
        ])
        run_consumer(consumer, Mock(), "course-cluster", replay_from_start=True, stop_event=stopped)
        self.assertEqual(len(consumer.assignments), 1)
        assigned = consumer.assignments[0]
        self.assertEqual([(part.topic, part.partition, part.offset) for part in assigned], [
            ("movielog2", 0, OFFSET_BEGINNING), ("movielog2", 1, OFFSET_BEGINNING),
        ])
        self.assertEqual(consumer.closed, 1)

    def test_envelope_normalizes_missing_broker_timestamp_and_optional_epoch(self):
        message = FakeMessage(timestamp=(0, -1))
        message.leader_epoch = None
        message.headers = lambda: None
        envelope = capture_envelope(message, "stable-cluster-id")
        self.assertEqual(envelope.source_id, "stable-cluster-id")
        self.assertIsNone(envelope.broker_timestamp_ms)
        self.assertEqual(envelope.broker_timestamp_type, 0)
        self.assertIsNone(envelope.leader_epoch)
        self.assertEqual(envelope.headers, [])


class KafkaConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "mlip-kafka.conf"

    def tearDown(self):
        self.temporary.cleanup()

    def test_required_safeguards_override_file_and_credentials_keep_equals(self):
        self.path.write_text("\ufeff# student config\n" + "\n".join([
            "bootstrap.servers=localhost:9092",
            "group.id=unsafe-file-group",
            "group.protocol=consumer",
            "enable.auto.commit=true",
            "enable.auto.offset.store=true",
            "auto.offset.reset=latest",
            "enable.partition.eof=true",
            "allow.auto.create.topics=true",
            "sasl.password=synthetic=secret==",
        ]), encoding="utf-8")
        config = load_kafka_config(self.path, "required-group")
        self.assertEqual(config["group.id"], "required-group")
        self.assertEqual(config["group.protocol"], "classic")
        self.assertIs(config["enable.auto.commit"], False)
        self.assertIs(config["enable.auto.offset.store"], False)
        self.assertEqual(config["auto.offset.reset"], "earliest")
        self.assertIs(config["enable.partition.eof"], False)
        self.assertIs(config["allow.auto.create.topics"], False)
        self.assertEqual(config["sasl.password"], "synthetic=secret==")
        self.assertIs(config["error_cb"], kafka_error)

    def test_defaults_use_9092_and_custom_broker_remains_configured(self):
        self.path.write_text("# empty configuration\n", encoding="utf-8")
        self.assertEqual(load_kafka_config(self.path, "group")["bootstrap.servers"], "localhost:9092")
        self.path.write_text("bootstrap.servers=broker.example:19092\n", encoding="utf-8")
        config = load_kafka_config(self.path, "group", "latest")
        self.assertEqual(config["bootstrap.servers"], "broker.example:19092")
        self.assertEqual(config["auto.offset.reset"], "latest")

    def test_invalid_config_and_required_group_are_rejected(self):
        for line in ("not-a-setting", "=missing-key"):
            with self.subTest(line=line):
                self.path.write_text(line, encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_kafka_config(self.path, "group")
        self.path.write_text("bootstrap.servers=localhost:9092", encoding="utf-8")
        with self.assertRaises(ValueError):
            load_kafka_config(self.path, " ")
        with self.assertRaises(ValueError):
            load_kafka_config(self.path, "group", "unsupported")

    def test_client_diagnostics_never_include_library_credentials(self):
        with self.assertLogs("events.consumer", level="ERROR") as captured:
            SafeKafkaLogger().log(logging.ERROR, "password=library-secret", "library-secret")
            kafka_error(SimpleNamespace(code=lambda: -187, __str__=lambda: "library-secret"))
        self.assertNotIn("library-secret", " ".join(captured.output))
        self.assertIn("-187", " ".join(captured.output))


if __name__ == "__main__":
    unittest.main()
