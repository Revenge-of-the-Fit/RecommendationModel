import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka import KafkaError, TopicPartition

from events.consumer import KafkaIngestionError, run_consumer
from events.parser import parse_event
from storage.events import EventStore, KafkaEnvelope
from storage.live import read_live_user
from experiments.capture import OutcomeCapture
from routing import write_json
from test_event_consumer import FakeConsumer, FakeMessage


class ScriptedConsumer(FakeConsumer):
    def __init__(self, steps, **kwargs):
        super().__init__(**kwargs)
        self.steps = iter(steps)
        self.now = 0

    def poll(self, timeout):
        self.polls.append(timeout)
        self.now, action = next(self.steps)
        return action() if callable(action) else action


def watch(offset, minute, timestamp, partition=2):
    message = FakeMessage(offset, f"{timestamp},42,GET /data/m/movie_a/{minute}.mpg".encode())
    message.partition = lambda: partition
    return message


def rating(offset, partition=2):
    message = FakeMessage(offset, b"2026-10-08T12:02:00Z,42,GET /rate/movie_b=8")
    message.partition = lambda: partition
    return message


class WatchConsumerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(self, consumer, **kwargs):
        with EventStore(self.path) as store, patch("events.consumer.time.monotonic", side_effect=lambda: consumer.now):
            return run_consumer(consumer, store, "cmu-movielog", **kwargs)

    def latest_minute(self):
        with EventStore(self.path) as store:
            watches = [row for row in store.list_events(42) if row["event_type"] == "watch"]
        return watches[0]["parsed"]["fields"]["minute"]

    def test_first_watch_is_live_and_later_rating_cannot_acknowledge_ram_updates(self):
        def inspect():
            self.assertEqual(self.latest_minute(), 1)
            self.assertIn("movie_a", read_live_user(self.path, 42)["seen"])
            self.assertEqual(consumer.commits[-1][0][0].offset, 101)
            with EventStore(self.path) as reader:
                self.assertIsNotNone(reader.get_event("cmu-movielog", "movielog2", 2, 103))
            return None

        consumer = ScriptedConsumer([
            (0, watch(100, 1, "2026-10-08T12:00:00Z")),
            (60, watch(101, 2, "2026-10-08T12:01:00Z")),
            (120, watch(102, 3, "2026-10-08T12:02:00Z")),
            (121, rating(103)), (122, inspect), (420, None),
        ])
        stats = self.run_script(consumer, batch_size=1, idle_timeout=299)
        self.assertEqual(self.latest_minute(), 3)
        self.assertEqual([parts[0].offset for parts, _ in consumer.commits], [101, 104])
        self.assertEqual(stats["received"], 4)
        self.assertEqual(stats["stored"], 3)
        self.assertEqual(stats["coalesced"], 1)
        self.assertEqual(stats["committed"], 4)

    def test_batch_transaction_visible_before_one_commit_covers_both_partitions(self):
        def inspect_commit(parts):
            with EventStore(self.path) as reader:
                self.assertEqual(len(reader.list_events(42)), 2)
            return FakeConsumer.commit(FakeConsumer(), offsets=parts, asynchronous=False)

        consumer = ScriptedConsumer([(0, rating(100, 0)), (0.01, rating(200, 1))], on_commit=inspect_commit)
        stats = self.run_script(consumer, batch_size=2, max_messages=2)
        self.assertEqual(len(consumer.commits), 1)
        self.assertEqual([(part.partition, part.offset) for part in consumer.commits[0][0]], [(0, 101), (1, 201)])
        self.assertEqual(stats["committed"], 2)

    def test_timer_flushes_first_watch_without_waiting_for_batch_to_fill(self):
        def inspect():
            self.assertEqual(self.latest_minute(), 1)
            return rating(101)

        consumer = ScriptedConsumer([(0, watch(100, 1, "2026-10-08T12:00:00Z")), (0.5, None), (0.6, inspect)])
        self.run_script(consumer, max_messages=2)
        self.assertEqual(consumer.commits[0][0][0].offset, 101)

    def test_unexpected_poll_failure_replays_uncommitted_updates_without_losing_newer_minute(self):
        failure = FakeMessage(error=KafkaError(KafkaError._ALL_BROKERS_DOWN))
        first = watch(100, 90, "2026-10-08T12:00:00Z")
        rewind = watch(101, 3, "2026-10-08T12:01:00Z")
        consumer = ScriptedConsumer([(0, first), (60, rewind), (61, rating(102)), (62, failure)])
        with self.assertRaises(KafkaIngestionError):
            self.run_script(consumer, batch_size=1)
        self.assertEqual(self.latest_minute(), 90)
        self.assertEqual(consumer.commits[-1][0][0].offset, 101)
        replay = ScriptedConsumer([(0, rewind), (1, rating(102))])
        stats = self.run_script(replay, batch_size=2, max_messages=2)
        self.assertEqual(self.latest_minute(), 3)
        self.assertEqual(stats["duplicates"], 1)
        self.assertEqual(stats["committed"], 2)

    def test_revoke_flushes_buffer_before_commit_but_lost_assignment_never_commits(self):
        for callback, expected_minute, expected_offsets in (
            ("on_revoke", 2, [101, 102]), ("on_lost", 1, [101]),
        ):
            with self.subTest(callback=callback):
                self.path = Path(self.temporary.name) / f"{callback}.sqlite3"
                def rebalance():
                    consumer.subscriptions[0][1][callback](consumer, [TopicPartition("movielog2", 2)])
                    return None
                consumer = ScriptedConsumer([
                    (0, watch(100, 1, "2026-10-08T12:00:00Z")),
                    (60, watch(101, 2, "2026-10-08T12:01:00Z")), (61, rebalance),
                ])
                self.run_script(consumer, batch_size=1, idle_timeout=1)
                self.assertEqual(self.latest_minute(), expected_minute)
                self.assertEqual([parts[0].offset for parts, _ in consumer.commits], expected_offsets)

    def test_failed_batch_rolls_back_first_watch_and_history_without_committing(self):
        class BrokenStore(EventStore):
            def _save_event(self, envelope, parsed):
                if envelope.offset == 101:
                    raise sqlite3.OperationalError("simulated failed write")
                return super()._save_event(envelope, parsed)

        consumer = ScriptedConsumer([(0, watch(100, 1, "2026-10-08T12:00:00Z")), (0.1, rating(101))])
        with BrokenStore(self.path) as store, patch("events.consumer.time.monotonic", side_effect=lambda: consumer.now):
            with self.assertRaises(sqlite3.OperationalError):
                run_consumer(consumer, store, "cmu-movielog", batch_size=2, max_messages=2)
            self.assertEqual(store.list_events(42), [])
        self.assertEqual(read_live_user(self.path, 42)["seen"], set())
        self.assertEqual(consumer.commits, [])

    def test_experiment_capture_keeps_middle_watch_before_buffer_and_database_compaction(self):
        directory = Path(self.temporary.name)
        plan = {"id": "raw-watches", "start": "2026-10-08T12:00:00+00:00",
                "enrollment_end": "2026-10-08T13:00:00+00:00", "follow_up_seconds": 3600,
                "observation_end": "2026-10-08T14:00:00+00:00", "control_slot": "blue",
                "treatment_percent": 50, "salt": "test", "versions": {"blue": "a", "green": "b"},
                "source_id": "cmu-movielog", "topic": "movielog2", "partitions": [2]}
        routing = directory / "routing.json"
        write_json(routing, {"primary": "blue", "secondary_percent": 50, "salt": "test", "generation": "test", "experiment": plan})
        capture = OutcomeCapture(directory / "experiment", routing)
        consumer = ScriptedConsumer([
            (0, watch(100, 1, "2026-10-08T12:00:00Z")),
            (60, watch(101, 2, "2026-10-08T12:01:00Z")),
            (120, watch(102, 3, "2026-10-08T12:02:00Z")),
        ])
        try:
            self.run_script(consumer, max_messages=3, observation_capture=capture)
        finally:
            capture.close()
        self.assertEqual(self.latest_minute(), 3)
        connection = sqlite3.connect(directory / "experiment" / "raw-watches" / "outcomes.sqlite3")
        try:
            self.assertEqual(connection.execute("SELECT value FROM observations ORDER BY offset").fetchall(), [(1,), (2,), (3,)])
        finally:
            connection.close()

    def test_failed_experiment_capture_never_acknowledges_unsaved_observations(self):
        class FailedCapture:
            def capture(self, records):
                raise sqlite3.OperationalError("simulated experiment write failure")
        consumer = ScriptedConsumer([(0, watch(100, 1, "2026-10-08T12:00:00Z"))])
        with self.assertRaises(sqlite3.OperationalError):
            self.run_script(consumer, max_messages=1, observation_capture=FailedCapture())
        self.assertEqual(consumer.commits, [])


if __name__ == "__main__":
    unittest.main()
