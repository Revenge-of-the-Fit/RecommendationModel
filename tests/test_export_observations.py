import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import export_observations as cli
from events.parser import parse_event
from storage.events import EventStore, KafkaEnvelope
from storage.operations import export_observations
from storage.requests import RequestStore


class ObservationExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"
        with RequestStore(self.path):
            pass

    def tearDown(self):
        self.temporary.cleanup()

    def request(self, request_id, stamp, movies=("movie_a",), **changes):
        record = {
            "request_id": request_id, "user_id": 42, "started_at": stamp,
            "finished_at": stamp, "status": 200, "response_complete": True,
            "recommendations": [{"movie_id": movie, "rank": rank, "score": rank / 10} for rank, movie in enumerate(movies, 1)],
            "serving_method": "ease", "versions": {"code": "commit", "model": "ease-1"},
            **changes,
        }
        with RequestStore(self.path) as requests:
            requests.save_request(record)

    def event(self, offset, stamp, body, *, source="course", topic="movielog2", ingested=None, user=42):
        value = f"{stamp},{user},{body}".encode("utf-8")
        item = KafkaEnvelope(source, topic, 0, offset, value, ingested_at=ingested or "2026-10-08T14:00:00+00:00")
        with EventStore(self.path) as events:
            events.save_event(item, parse_event(value))

    def export(self, **options):
        output = io.StringIO()
        counts = export_observations(self.path, output, **options)
        return counts, [json.loads(line) for line in output.getvalue().splitlines()]

    def test_multiple_impressions_remain_candidates_and_movie_minutes_are_not_plays(self):
        self.request("first", "2026-10-08T12:00:00+00:00")
        self.request("second", "2026-10-08T12:01:00+00:00")
        self.event(1, "2026-10-08T12:02:00+00:00", "GET /data/m/movie_a/17.mpg")
        self.event(2, "2026-10-08T12:03:00+00:00", "GET /rate/movie_a=9")
        counts, rows = self.export()
        self.assertEqual(counts, {"impressions": 2, "observed_events": 2, "candidate_links": 4, "ambiguous_events": 2})
        events = [row for row in rows if row["record_type"] == "observed_event"]
        self.assertTrue(all(row["ambiguous"] and row["candidate_count"] == 2 and row["attribution"] == "candidate_only" for row in events))
        self.assertEqual(events[0]["observation_unit"], "latest_requested_minute")
        self.assertEqual(events[0]["observed_minute"], 17)
        self.assertNotIn("plays", events[0])
        self.assertEqual(events[1]["rating"], 9)
        links = [row for row in rows if row["record_type"] == "candidate_link"]
        self.assertEqual({row["impression_identity"]["request_id"] for row in links}, {"first", "second"})
        self.assertEqual({row["seconds_after_impression"] for row in links}, {60, 120, 180})
        self.assertTrue(all(row["matching_basis"] == "same_user_movie_and_later_timestamp" for row in links))

    def test_single_or_zero_matches_do_not_claim_attribution_and_failed_requests_do_not_match(self):
        self.request("good", "2026-10-08T12:00:00+00:00")
        self.request("failed", "2026-10-08T12:00:30+00:00", status=503)
        self.request("incomplete", "2026-10-08T12:00:30+00:00", response_complete=False)
        self.event(1, "2026-10-08T12:01:00+00:00", "GET /rate/movie_a=8")
        self.event(2, "2026-10-08T12:01:00+00:00", "GET /rate/movie_b=8")
        self.event(3, "2026-10-08T12:01:00+00:00", "GET /rate/movie_a=8", user=43)
        counts, rows = self.export()
        self.assertEqual(counts["impressions"], 1)
        self.assertEqual(counts["candidate_links"], 1)
        events = [row for row in rows if row["record_type"] == "observed_event"]
        self.assertEqual([row["candidate_count"] for row in events], [1, 0, 0])
        self.assertTrue(all(not row["ambiguous"] and row["attribution"] == "candidate_only" for row in events))

    def test_kafka_impressions_preserve_duplicate_ranked_ids_and_source_namespace(self):
        self.event(1, "2026-10-08T12:00:00+00:00", "recommendation request server-a, status 200, result: movie_a,movie_a,movie_b, 12 ms")
        self.event(1, "2026-10-08T12:00:30+00:00", "recommendation request server-b, status 200, result: movie_a, 12 ms", source="other")
        self.event(2, "2026-10-08T12:01:00+00:00", "GET /data/m/movie_a/1.mpg")
        counts, rows = self.export(source_id="course", topic="movielog2")
        self.assertEqual(counts["impressions"], 1)
        self.assertEqual(counts["candidate_links"], 1)
        impression = rows[0]
        self.assertEqual([item["movie_id"] for item in impression["recommendations"]], ["movie_a", "movie_a", "movie_b"])
        self.assertEqual(impression["identity"], {"source_id": "course", "topic": "movielog2", "partition": 0, "offset": 1})
        link = next(row for row in rows if row["record_type"] == "candidate_link")
        self.assertEqual([item["rank"] for item in link["recommendations"]], [1, 2])
        self.assertEqual(link["candidate_count"], 1)

    def test_asof_filters_both_ingestion_and_completed_impressions(self):
        self.request("known", "2026-10-08T12:00:00+00:00")
        self.request("not-complete", "2026-10-08T12:00:00+00:00", finished_at="2026-10-08T13:00:00+00:00")
        self.event(1, "2026-10-08T12:01:00+00:00", "GET /data/m/movie_a/1.mpg", ingested="2026-10-08T12:01:05+00:00")
        self.event(2, "2026-10-08T12:02:00+00:00", "GET /rate/movie_a=8", ingested="2026-10-08T15:00:00+00:00")
        self.event(3, "2026-10-08T11:59:00+00:00", "recommendation request late, status 200, result: movie_a, 1 ms", ingested="2026-10-08T15:00:00+00:00")
        counts, rows = self.export(as_of="2026-10-08T12:30:00+00:00")
        self.assertEqual(counts, {"impressions": 1, "observed_events": 1, "candidate_links": 1, "ambiguous_events": 0})
        self.assertEqual(rows[-1]["impression_identity"], {"request_id": "known"})

    def test_window_requires_strictly_later_event_and_honors_time_and_topic_filters(self):
        self.request("first", "2026-10-08T12:00:00+00:00")
        self.event(1, "2026-10-08T12:00:00+00:00", "GET /rate/movie_a=8")
        self.event(2, "2026-10-08T12:01:00+00:00", "GET /rate/movie_a=8")
        self.event(3, "2026-10-08T12:01:01+00:00", "GET /rate/movie_a=8")
        self.event(4, "2026-10-08T12:00:30+00:00", "GET /rate/movie_a=8", topic="other")
        counts, rows = self.export(topic="movielog2", match_window_seconds=60, start="2026-10-08T12:00:00+00:00", end="2026-10-08T12:01:01+00:00")
        self.assertEqual(counts["observed_events"], 2)
        self.assertEqual(counts["candidate_links"], 1)
        self.assertEqual(rows[-1]["seconds_after_impression"], 60)
        self.assertEqual(rows[-1]["observed_identity"]["offset"], 2)

    def test_naive_or_invalid_events_and_error_recommendations_are_not_invented_as_matches(self):
        self.request("first", "2026-10-08T12:00:00+00:00")
        self.event(1, "2026-10-08T12:01:00", "GET /data/m/movie_a/1.mpg")
        self.event(2, "2026-10-08T12:01:00+00:00", "GET /rate/movie_a=11")
        self.event(3, "2026-10-08T11:59:00+00:00", "recommendation request error, status 503, result: movie_a, 1 ms")
        counts, _ = self.export()
        self.assertEqual(counts, {"impressions": 1, "observed_events": 0, "candidate_links": 0, "ambiguous_events": 0})

    def test_cli_streams_jsonl_separately_from_counts_and_explains_ambiguity(self):
        self.request("first", "2026-10-08T12:00:00+00:00")
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            self.assertEqual(cli.main(["--storage-path", str(self.path)]), 0)
        self.assertEqual(json.loads(output.getvalue())["record_type"], "impression")
        self.assertEqual(json.loads(errors.getvalue())["impressions"], 1)
        help_text = cli.build_parser().format_help()
        self.assertIn("does not prove attribution", help_text)
        self.assertIn("never play counts", help_text)
        for options in ({"match_window_seconds": 0}, {"match_window_seconds": float("nan")}, {"match_window_seconds": True}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.export(**options)


if __name__ == "__main__":
    unittest.main()
