import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from services.metadata import MetadataBatch, MetadataCollector, MetadataRateLimiter
from storage.metadata import MetadataStore


class FakeClock:
    def __init__(self):
        self.current = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
        self.elapsed = 0.0
        self.waits = []

    def now(self):
        return self.current

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.advance(seconds)

    def advance(self, seconds):
        self.current += timedelta(seconds=seconds)
        self.elapsed += seconds


class MetadataCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = MetadataStore(Path(self.temporary.name) / "events.sqlite3")
        self.clock = FakeClock()
        self.fetcher = Mock(side_effect=lambda kind, ids: MetadataBatch(
            200, {entity_id: {"id": entity_id, "extra": {"kind": kind}} for entity_id in ids},
            {"headers": {"x-trace-id": "trace-1"}, "body": {"ids": ids}},
        ))
        self.limiter = MetadataRateLimiter(monotonic=self.clock.monotonic, sleep=self.clock.sleep)

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def collector(self, **changes):
        return MetadataCollector(self.store, "course-api", self.fetcher, clock=self.clock.now, limiter=self.limiter, **changes)

    def test_batches_deduplicate_ids_preserve_order_and_enforce_200_limit(self):
        ids = list(range(1, 402)) + ["001", 401]
        result = self.collector().collect("user", ids)
        batches = [call.args[1] for call in self.fetcher.call_args_list]
        self.assertEqual([len(batch) for batch in batches], [200, 200, 1])
        self.assertEqual([item for batch in batches for item in batch], [str(i) for i in range(1, 402)])
        self.assertEqual(result.fetched, 401)
        self.assertEqual(result.unresolved, [])
        self.assertEqual(len(result.fetch_ids), 3)
        self.assertEqual(self.clock.waits, [1.0, 1.0])
        for fetch_id in result.fetch_ids:
            fetch = self.store.get_fetch(fetch_id)
            self.assertEqual(fetch["status"], "success")
            self.assertLessEqual(len(fetch["requested_ids"]), 200)

    def test_movie_ids_are_preserved_without_numeric_conversion(self):
        ids = ["american+beauty+1999", "moolaad+2004", "movie,with,commas", "🎬"]
        result = self.collector().collect("movie", ids)
        self.assertEqual(self.fetcher.call_args.args, ("movie", ids))
        self.assertEqual(list(result.snapshots), ids)
        self.assertEqual(result.snapshots["🎬"]["record"]["extra"], {"kind": "movie"})

    def test_fresh_cache_is_reused_and_exact_ttl_boundary_refreshes(self):
        collector = self.collector(max_age_seconds=60)
        first = collector.collect("user", [42])
        self.clock.advance(59)
        cached = collector.collect("user", [42])
        self.assertEqual(cached.cached, 1)
        self.assertEqual(cached.fetched, 0)
        self.assertEqual(cached.fetch_ids, [])
        self.assertEqual(cached.snapshots["42"], first.snapshots["42"])
        self.clock.advance(1)
        refreshed = collector.collect("user", [42])
        self.assertEqual(self.fetcher.call_count, 2)
        self.assertEqual(refreshed.fetched, 1)
        self.assertNotEqual(refreshed.snapshots["42"]["snapshot_id"], first.snapshots["42"]["snapshot_id"])
        self.assertEqual(refreshed.snapshots["42"]["content_version"], first.snapshots["42"]["content_version"])
        self.assertIsNotNone(self.store.get_snapshot(first.snapshots["42"]["snapshot_id"]))

    def test_force_refresh_records_new_observation_and_description_version(self):
        collector = self.collector()
        first = collector.collect("user", [42])
        self.clock.advance(1)
        self.fetcher.side_effect = lambda kind, ids: MetadataBatch(200, {"42": {"self_description_likes": "science fiction", "self_description_dislikes": ""}})
        refreshed = collector.collect("user", [42], force=True)
        self.assertNotEqual(refreshed.snapshots["42"]["content_version"], first.snapshots["42"]["content_version"])
        self.assertEqual(refreshed.snapshots["42"]["record"]["self_description_dislikes"], "")
        historical = self.store.latest_snapshot("course-api", "user", 42, as_of=self.clock.current - timedelta(seconds=1))
        self.assertEqual(historical, first.snapshots["42"])

    def test_offline_cache_only_mode_never_fetches_or_sleeps(self):
        collector = self.collector(max_age_seconds=60)
        collector.collect("user", [42])
        self.fetcher.reset_mock()
        cached = collector.collect("user", [42, 43], offline=True)
        self.assertEqual(cached.cached, 1)
        self.assertEqual(cached.unresolved, ["43"])
        self.clock.advance(60)
        expired = collector.collect("user", [42, 43], offline=True)
        self.assertEqual(expired.snapshots, {})
        self.assertEqual(expired.unresolved, ["42", "43"])
        self.fetcher.assert_not_called()
        self.assertEqual(self.clock.waits, [])

    def test_future_snapshots_are_not_available_to_cache_reads(self):
        collector = self.collector()
        collector.collect("user", [42])
        self.clock.advance(-1)
        result = collector.collect("user", [42], offline=True)
        self.assertEqual(result.snapshots, {})
        self.assertEqual(result.unresolved, ["42"])

    def test_partial_results_preserve_response_and_report_missing_ids(self):
        body = {"records": [{"id": "42", "unknown": {"nested": [1, 2]}}], "extra": "source field"}
        self.fetcher.side_effect = None
        self.fetcher.return_value = MetadataBatch(200, {"42": body["records"][0]}, body)
        result = self.collector().collect("user", [42, 43])
        self.assertEqual(result.fetched, 1)
        self.assertEqual(result.unresolved, ["43"])
        self.assertEqual(result.snapshots["42"]["record"]["unknown"], {"nested": [1, 2]})
        fetch = self.store.get_fetch(result.fetch_ids[0])
        self.assertEqual(fetch["status"], "partial")
        self.assertEqual(fetch["response"], body)
        self.assertEqual(fetch["missing_ids"], ["43"])

    def test_transport_failure_is_durable_and_keeps_prior_snapshot(self):
        collector = self.collector()
        original = collector.collect("user", [42]).snapshots["42"]
        self.clock.advance(1)
        self.fetcher.side_effect = TimeoutError("Bearer credential-sentinel")
        with self.assertLogs("services.metadata", level="WARNING") as output:
            result = collector.collect("user", [42], force=True)
        fetch = self.store.get_fetch(result.fetch_ids[0])
        self.assertEqual(fetch["status"], "failed")
        self.assertEqual(fetch["error_type"], "TimeoutError")
        self.assertEqual(fetch["response"], None)
        self.assertEqual(result.unresolved, ["42"])
        self.assertEqual(self.store.latest_snapshot("course-api", "user", 42), original)
        self.assertNotIn("credential-sentinel", str(fetch) + str(output.output))

    def test_http_error_and_empty_response_are_not_successful_snapshots(self):
        for batch, error in (
            (MetadataBatch(429, {"42": {"id": "42"}}, {"retry_after": 2}), "HttpError"),
            (MetadataBatch(200), "MissingMetadata"),
            (MetadataBatch(None, error_type="ConnectionError"), "ConnectionError"),
        ):
            with self.subTest(batch=batch):
                self.fetcher.side_effect = None
                self.fetcher.return_value = batch
                result = self.collector().collect("user", [42], force=True)
                fetch = self.store.get_fetch(result.fetch_ids[0])
                self.assertEqual(fetch["status"], "failed")
                self.assertEqual(fetch["error_type"], error)
                self.assertEqual(result.snapshots, {})
                self.assertIsNone(self.store.latest_snapshot("course-api", "user", 42))

    def test_unrequested_duplicate_and_invalid_records_are_rejected(self):
        for records in ({"43": {}}, {"42": {}, "042": {}}, {"42": []}, [], {True: {}}):
            with self.subTest(records=records):
                self.fetcher.side_effect = None
                self.fetcher.return_value = MetadataBatch(200, records, {"body": "original response"})
                result = self.collector().collect("user", [42], force=True)
                fetch = self.store.get_fetch(result.fetch_ids[0])
                self.assertEqual(fetch["error_type"], "ValueError")
                self.assertEqual(fetch["response"], {"body": "original response"})
                self.assertEqual(result.unresolved, ["42"])
                self.assertEqual(result.snapshots, {})

    def test_limiter_is_shared_across_user_movie_and_collector_calls(self):
        self.collector().collect("user", [42])
        self.collector().collect("movie", ["movie_a"])
        self.collector().collect("user", [43])
        self.assertEqual(self.clock.waits, [1.0, 1.0])

    def test_invalid_entity_values_preserve_the_failed_fetch_envelope(self):
        response = {"body": "original response", "headers": {"x-trace-id": "trace-1"}}
        for value, category in ((float("nan"), "ValueError"), (object(), "TypeError")):
            with self.subTest(category=category):
                self.fetcher.side_effect = None
                self.fetcher.return_value = MetadataBatch(200, {"42": {"score": value}}, response)
                result = self.collector().collect("user", [42], force=True)
                fetch = self.store.get_fetch(result.fetch_ids[0])
                self.assertEqual(fetch["status"], "failed")
                self.assertEqual(fetch["error_type"], category)
                self.assertEqual(fetch["response"], response)
                self.assertEqual(result.snapshots, {})
                self.assertEqual(result.unresolved, ["42"])
                self.assertIsNone(self.store.latest_snapshot("course-api", "user", 42))

    def test_empty_input_and_invalid_ids_do_not_call_the_fetcher(self):
        self.assertEqual(self.collector().collect("user", []).fetch_ids, [])
        for kind, ids in (("user", [0]), ("user", [True]), ("user", [1, "bad"]), ("movie", [""]), ("invalid", [42])):
            with self.subTest(kind=kind, ids=ids):
                with self.assertRaises(ValueError):
                    self.collector().collect(kind, ids)
        self.fetcher.assert_not_called()

    def test_invalid_configuration_is_rejected(self):
        for changes in ({"batch_size": 201}, {"batch_size": 0}, {"batch_size": True}, {"max_age_seconds": 0}, {"max_age_seconds": float("nan")}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.collector(**changes)
        for interval in (-1, True, float("inf")):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                MetadataRateLimiter(interval)


if __name__ == "__main__":
    unittest.main()
