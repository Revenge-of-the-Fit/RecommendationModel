import sys
import unittest
from dataclasses import replace
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from events.watch_buffer import WatchBuffer
from storage.events import KafkaEnvelope


class WatchBufferTests(unittest.TestCase):
    @staticmethod
    def watch(offset, minute=17, timestamp="2026-10-10T12:00:00+00:00", user_id=42,
              movie_id="movie_a", partition=0, **changes):
        envelope = KafkaEnvelope(
            source_id=changes.pop("source_id", "cmu-movielog"),
            topic=changes.pop("topic", "movielog2"), partition=partition, offset=offset,
            value=f"{timestamp},{user_id},GET /data/m/{movie_id}/{minute}.mpg".encode(), **changes,
        )
        return envelope, parse_event(envelope.value)

    def test_first_watch_ready_and_latest_held_without_per_heartbeat_growth(self):
        buffer = WatchBuffer()
        first = self.watch(10)
        self.assertEqual(buffer.process(*first, now=0, sequence=0), [first])
        records = []
        for index in range(1, 101):
            latest = self.watch(10 + index, minute=index, timestamp=f"2026-10-10T12:01:{index % 60:02d}+00:00")
            records.append(latest)
            buffer.process(*latest, now=index, sequence=index)
        self.assertEqual(buffer.active_count, 1)
        self.assertEqual(buffer.buffered_count, 1)
        self.assertEqual(buffer.coalesced, 99)
        self.assertEqual(buffer.blocked_positions(), {("movielog2", 0): (11, 1)})
        expected = max(records, key=lambda record: record[1]["event_timestamp"])
        self.assertEqual(buffer.flush(), [expected])
        self.assertEqual(len(buffer), 0)
        self.assertEqual(buffer.blocked_positions(), {})

    def test_idle_expiry_returns_latest_and_removes_clean_sessions(self):
        buffer = WatchBuffer(idle_seconds=300)
        first, latest = self.watch(1), self.watch(2, minute=18, timestamp="2026-10-10T12:01:00Z")
        buffer.process(*first, now=0, sequence=0)
        buffer.process(*latest, now=60, sequence=1)
        clean = self.watch(3, user_id=43)
        buffer.process(*clean, now=10, sequence=2)
        self.assertEqual(buffer.expire(310), [])
        self.assertEqual(len(buffer), 1)
        self.assertEqual(buffer.expire(359), [])
        self.assertEqual(buffer.expire(360), [latest])
        self.assertEqual(buffer.blocked_positions(), {})

    def test_latest_timestamp_wins_even_when_minute_rewinds_or_stale_arrives(self):
        buffer = WatchBuffer()
        first = self.watch(1, minute=90)
        rewind = self.watch(2, minute=3, timestamp="2026-10-10T12:01:00Z")
        stale = self.watch(999, minute=120, timestamp="2026-10-10T12:00:30Z", partition=1)
        buffer.process(*first, now=0, sequence=0)
        buffer.process(*rewind, now=1, sequence=1)
        buffer.process(*stale, now=2, sequence=0)
        self.assertEqual(buffer.blocked_positions(), {
            ("movielog2", 0): (2, 1), ("movielog2", 1): (999, 0),
        })
        self.assertEqual(buffer.coalesced, 1)
        self.assertEqual(buffer.flush(), [rewind])

    def test_clean_session_stale_record_is_still_counted_at_flush(self):
        buffer = WatchBuffer()
        latest = self.watch(1, timestamp="2026-10-10T12:01:00Z")
        stale = self.watch(2, timestamp="2026-10-10T12:00:00Z")
        buffer.process(*latest, now=0, sequence=0)
        self.assertEqual(buffer.process(*stale, now=1, sequence=1), [])
        self.assertEqual(buffer.coalesced, 0)
        self.assertEqual(buffer.flush(), [latest])

    def test_blocking_offsets_are_minimum_per_partition_across_sessions(self):
        buffer = WatchBuffer()
        a_first, a_latest = self.watch(10), self.watch(12, timestamp="2026-10-10T12:01:00Z")
        b_first, b_latest = self.watch(11, user_id=43), self.watch(13, user_id=43, timestamp="2026-10-10T12:01:30Z")
        other_partition = self.watch(9, user_id=43, partition=1, timestamp="2026-10-10T12:01:40Z")
        for record, now, sequence in ((a_first, 0, 0), (b_first, 1, 1),
                                      (a_latest, 60, 2), (b_latest, 90, 3), (other_partition, 100, 0)):
            buffer.process(*record, now=now, sequence=sequence)
        self.assertEqual(buffer.blocked_positions(), {
            ("movielog2", 0): (12, 2), ("movielog2", 1): (9, 0),
        })
        self.assertEqual(buffer.expire(360), [a_latest])
        self.assertEqual(buffer.blocked_positions(), {
            ("movielog2", 0): (13, 3), ("movielog2", 1): (9, 0),
        })
        self.assertEqual(buffer.flush(), [other_partition])

    def test_replay_event_gap_and_watermark_flush_without_wall_clock_wait(self):
        buffer = WatchBuffer()
        first = self.watch(1)
        latest = self.watch(2, timestamp="2026-10-10T12:01:00Z")
        next_session = self.watch(3, timestamp="2026-10-10T12:06:00Z")
        buffer.process(*first, now=0, sequence=0)
        buffer.process(*latest, now=0.1, sequence=1)
        self.assertEqual(buffer.process(*next_session, now=0.2, sequence=2), [latest, next_session])
        newer = self.watch(4, timestamp="2026-10-10T12:07:00Z")
        buffer.process(*newer, now=0.3, sequence=3)
        from datetime import datetime
        watermark = datetime.fromisoformat("2026-10-10T12:12:00Z").timestamp() * 1000
        self.assertEqual(buffer.expire(0.4, watermark), [newer])
        self.assertEqual(len(buffer), 0)

    def test_capacity_flushes_least_recent_session_before_eviction(self):
        buffer = WatchBuffer(max_sessions=2)
        a, b = self.watch(1), self.watch(2, user_id=43)
        a_latest = self.watch(3, timestamp="2026-10-10T12:01:00Z")
        b_latest = self.watch(4, user_id=43, timestamp="2026-10-10T12:01:00Z")
        c = self.watch(5, user_id=44)
        buffer.process(*a, now=0, sequence=0)
        buffer.process(*b, now=1, sequence=1)
        buffer.process(*a_latest, now=2, sequence=2)
        buffer.process(*b_latest, now=3, sequence=3)
        self.assertEqual(buffer.process(*c, now=4, sequence=4), [a_latest, c])
        self.assertEqual(len(buffer), 2)
        self.assertEqual(buffer.blocked_positions(), {("movielog2", 0): (4, 3)})
        self.assertEqual(buffer.flush(), [b_latest])

    def test_nonwatch_and_invalid_watch_pass_through_and_namespace_isolated(self):
        buffer = WatchBuffer()
        for body in ("GET /rate/movie_a=8", "GET /data/m/movie_a/-1.mpg", "future format"):
            envelope, _ = self.watch(1)
            envelope = replace(envelope, value=f"2026-10-10T12:00:00Z,42,{body}".encode())
            parsed = parse_event(envelope.value)
            self.assertEqual(buffer.process(envelope, parsed, 0, 0), [(envelope, parsed)])
        for changes in ({}, {"source_id": "other"}, {"topic": "other"}, {"movie_id": "movie_b"}):
            record = self.watch(2, **changes)
            self.assertEqual(buffer.process(*record, now=1, sequence=0), [record])
        self.assertEqual(len(buffer), 4)
        self.assertEqual(buffer.flush(), [])

    def test_broker_time_fallback_and_missing_clock_wall_expiry(self):
        buffer = WatchBuffer()
        first = self.watch(1, timestamp="2026-10-10T12:00:00", broker_timestamp_ms=1000)
        latest = self.watch(2, timestamp="2026-10-10T12:00:00", broker_timestamp_ms=2000)
        buffer.process(*first, now=0, sequence=0)
        buffer.process(*latest, now=1, sequence=1)
        self.assertEqual(buffer.expire(2, 302000), [latest])
        undated = self.watch(3, timestamp="2026-10-10T12:00:00")
        undated_latest = self.watch(4, timestamp="2026-10-10T12:00:00")
        buffer.process(*undated, now=3, sequence=2)
        buffer.process(*undated_latest, now=4, sequence=3)
        self.assertEqual(buffer.expire(5, 9999999999999), [])
        self.assertEqual(buffer.expire(304), [undated_latest])

    def test_invalid_limits_rejected(self):
        for arguments in ({"idle_seconds": 0}, {"idle_seconds": float("inf")},
                          {"max_sessions": 0}, {"max_sessions": 1.5}, {"max_sessions": True}):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                WatchBuffer(**arguments)


if __name__ == "__main__":
    unittest.main()
