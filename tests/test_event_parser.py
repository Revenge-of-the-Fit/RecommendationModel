import json
import sys
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event


def event_value(body, timestamp="2026-10-08T12:00:00+00:00", user_id="42"):
    return f"{timestamp},{user_id},{body}".encode("utf-8")


class EventParserTests(unittest.TestCase):
    def test_recommendation_lists_allow_quoted_ids_without_changing_order(self):
        for result_text in ('["movie_a", "movie_b"]', "['movie_a', 'movie_b']", "[movie_a,movie_b]"):
            with self.subTest(result_text=result_text):
                parsed = parse_event(event_value(
                    f"recommendation request team2, status 200, result: {result_text}, 5 ms"
                ))
                self.assertEqual(parsed["parse_status"], "parsed")
                self.assertEqual(parsed["fields"]["recommendations"], [
                    {"movie_id": "movie_a", "rank": 1}, {"movie_id": "movie_b", "rank": 2},
                ])
                self.assertEqual(parsed["fields"]["result_text"], result_text)

    def test_watch_is_an_observed_minute(self):
        parsed = parse_event(event_value("GET /data/m/movie_name+2026/17.mpg"))
        self.assertEqual(parsed["parse_status"], "parsed")
        self.assertEqual(parsed["parser_version"], 1)
        self.assertIsNone(parsed["error_type"])
        self.assertEqual(parsed["event_type"], "watch")
        self.assertEqual(parsed["user_id"], 42)
        self.assertEqual(parsed["movie_id"], "movie_name+2026")
        self.assertEqual(parsed["fields"], {"minute": 17, "body": "GET /data/m/movie_name+2026/17.mpg"})
        self.assertEqual(parsed["event_timestamp"], "2026-10-08T12:00:00+00:00")
        self.assertEqual(parsed["event_timestamp_raw"], "2026-10-08T12:00:00+00:00")
        self.assertEqual(parsed["timestamp_status"], "utc")

    def test_rating_preserves_inclusive_boundaries(self):
        for rating in (1, 10):
            with self.subTest(rating=rating):
                parsed = parse_event(event_value(f"GET /rate/movie_a={rating}"))
                self.assertEqual(parsed["parse_status"], "parsed")
                self.assertEqual(parsed["event_type"], "rating")
                self.assertEqual(parsed["movie_id"], "movie_a")
                self.assertEqual(parsed["fields"], {"rating": rating, "body": f"GET /rate/movie_a={rating}"})

    def test_invalid_ratings_and_watch_grammar_fail_without_throwing(self):
        cases = {
            "GET /rate/movie_a=0": "InvalidRating",
            "GET /rate/movie_a=11": "InvalidRating",
            "GET /rate/movie_a=-1": "InvalidRating",
            "GET /rate/movie_a=1.5": "InvalidRatingEvent",
            "GET /rate/movie_a=not-a-rating": "InvalidRatingEvent",
            "GET /data/m/movie_a/-1.mpg": "InvalidWatchMinute",
            "GET /data/m/movie_a/not-a-minute.mpg": "InvalidWatchEvent",
            "GET /data/m/movie_a/1.mpg/extra": "InvalidWatchEvent",
        }
        for body, category in cases.items():
            with self.subTest(body=body):
                parsed = parse_event(event_value(body))
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["error_type"], category)
                json.dumps(parsed, allow_nan=False)

    def test_account_event_requires_exact_body(self):
        parsed = parse_event(event_value("GET /create_account"))
        self.assertEqual(parsed["parse_status"], "parsed")
        self.assertEqual(parsed["event_type"], "account_created")
        self.assertIsNone(parsed["movie_id"])
        for body in ("GET /create_account?role=admin", "GET /create_account/", "POST /create_account"):
            with self.subTest(body=body):
                unknown = parse_event(event_value(body))
                self.assertEqual(unknown["parse_status"], "unrecognized")
                self.assertIsNone(unknown["event_type"])
                self.assertEqual(unknown["fields"]["body"], body)

    def test_recommendation_results_preserve_order_duplicates_and_server(self):
        for received in ("movie_b,movie_a,movie_b", "[movie_b, movie_a, movie_b]"):
            with self.subTest(received=received):
                parsed = parse_event(event_value(
                    f"recommendation request server-1:8082, status 200, result: {received}, 12.5 ms"
                ))
                self.assertEqual(parsed["parse_status"], "parsed")
                self.assertEqual(parsed["event_type"], "recommendation")
                self.assertEqual(parsed["fields"]["server"], "server-1:8082")
                self.assertEqual(parsed["fields"]["status"], 200)
                self.assertEqual(parsed["fields"]["result_text"], received)
                self.assertEqual(parsed["fields"]["response_time_raw"], "12.5 ms")
                self.assertEqual(parsed["fields"]["response_time_ms"], 12.5)
                self.assertEqual(parsed["fields"]["recommendations"], [
                    {"movie_id": "movie_b", "rank": 1},
                    {"movie_id": "movie_a", "rank": 2},
                    {"movie_id": "movie_b", "rank": 3},
                ])

    def test_non200_keeps_error_text_without_inventing_movie_ids(self):
        parsed = parse_event(event_value(
            "recommendation request server-1, status 503, result: Service unavailable, retry later, 1s"
        ))
        self.assertEqual(parsed["parse_status"], "parsed")
        self.assertEqual(parsed["fields"]["result_text"], "Service unavailable, retry later")
        self.assertEqual(parsed["fields"]["status"], 503)
        self.assertEqual(parsed["fields"]["response_time_ms"], 1000)
        self.assertNotIn("recommendations", parsed["fields"])
        self.assertIsNone(parsed["movie_id"])

    def test_response_duration_requires_explicit_recognized_units(self):
        cases = {"2 seconds": 2000, "1500 us": 1.5, "0 milliseconds": 0, "0.125s": 125}
        for raw, milliseconds in cases.items():
            with self.subTest(raw=raw):
                parsed = parse_event(event_value(
                    f"recommendation request server-1, status 200, result: movie_a, {raw}"
                ))
                self.assertEqual(parsed["fields"]["response_time_raw"], raw)
                self.assertEqual(parsed["fields"]["response_time_ms"], milliseconds)
        for raw in ("12.5", "unknown", "10 ticks"):
            with self.subTest(raw=raw):
                parsed = parse_event(event_value(
                    f"recommendation request server-1, status 200, result: movie_a, {raw}"
                ))
                self.assertEqual(parsed["parse_status"], "parsed")
                self.assertEqual(parsed["fields"]["response_time_raw"], raw)
                self.assertNotIn("response_time_ms", parsed["fields"])

    def test_malformed_recommendations_have_safe_failure_categories(self):
        cases = {
            "recommendation request server-1, status 200, result: movie_a": "InvalidRecommendationEvent",
            "recommendation request server-1, status 999, result: movie_a, 2ms": "InvalidRecommendationStatus",
            "recommendation request server-1, status 200, result: [movie_a, 2ms": "InvalidRecommendationResult",
            "recommendation request server-1, status 200, result: movie a, 2ms": "InvalidRecommendationResult",
        }
        for body, category in cases.items():
            with self.subTest(body=body):
                parsed = parse_event(event_value(body))
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["error_type"], category)

    def test_timestamp_offsets_are_normalized_without_overriding_embedded_zone(self):
        parsed = parse_event(event_value("GET /create_account", "2026-10-08T08:00:00-04:00"), "+05:30")
        self.assertEqual(parsed["event_timestamp"], "2026-10-08T12:00:00+00:00")
        self.assertEqual(parsed["timestamp_status"], "utc")
        self.assertEqual(parsed["event_timestamp_raw"], "2026-10-08T08:00:00-04:00")
        for timestamp in ("2026-10-08T12:00:00Z", "2026-10-08T12:00:00.125+00:00"):
            with self.subTest(timestamp=timestamp):
                self.assertEqual(parse_event(event_value("GET /create_account", timestamp))["timestamp_status"], "utc")

    def test_naive_timestamp_remains_missing_zone_unless_explicitly_configured(self):
        value = event_value("GET /create_account", "2026-10-08 12:00:00")
        parsed = parse_event(value)
        self.assertEqual(parsed["parse_status"], "parsed")
        self.assertEqual(parsed["timestamp_status"], "timezone_missing")
        self.assertIsNone(parsed["event_timestamp"])
        self.assertIsNone(parsed["error_type"])
        self.assertEqual(parse_event(value, "UTC")["event_timestamp"], "2026-10-08T12:00:00+00:00")
        self.assertEqual(parse_event(value, "+05:30")["event_timestamp"], "2026-10-08T06:30:00+00:00")
        self.assertEqual(parse_event(value, "-0400")["event_timestamp"], "2026-10-08T16:00:00+00:00")
        invalid = parse_event(value, "+24:00")
        self.assertEqual(invalid["parse_status"], "failed")
        self.assertEqual(invalid["timestamp_status"], "invalid")
        self.assertEqual(invalid["error_type"], "InvalidTimezone")

    def test_iana_timezone_is_respected_when_available(self):
        try:
            ZoneInfo("America/New_York")
        except ZoneInfoNotFoundError:
            self.skipTest("IANA timezone data is unavailable")
        parsed = parse_event(event_value("GET /create_account", "2026-10-08 08:00:00"), "America/New_York")
        self.assertEqual(parsed["event_timestamp"], "2026-10-08T12:00:00+00:00")
        for timestamp, category in (
            ("2026-03-08 02:30:00", "InvalidTimestamp"),
            ("2026-11-01 01:30:00", "AmbiguousTimestamp"),
        ):
            with self.subTest(timestamp=timestamp):
                parsed = parse_event(event_value("GET /create_account", timestamp), "America/New_York")
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["error_type"], category)

    def test_invalid_and_missing_timestamps_retain_recognized_event(self):
        for timestamp, status, category in (("bad-time", "invalid", "InvalidTimestamp"), ("", "missing", "MissingTimestamp")):
            with self.subTest(timestamp=timestamp):
                parsed = parse_event(event_value("GET /rate/movie_a=7", timestamp))
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["timestamp_status"], status)
                self.assertEqual(parsed["error_type"], category)
                self.assertEqual(parsed["event_type"], "rating")
                self.assertEqual(parsed["fields"]["rating"], 7)
                self.assertIsNone(parsed["event_timestamp"])

    def test_user_id_must_fit_positive_sqlite_integer(self):
        for raw in ("0", "-1", "not-a-user", str(2**63), "9" * 5000):
            with self.subTest(raw=raw[:40]):
                parsed = parse_event(event_value("GET /create_account", user_id=raw))
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["error_type"], "InvalidUserID")
                self.assertIsNone(parsed["user_id"])
                self.assertEqual(parsed["fields"]["user_id_raw"], raw)
        self.assertEqual(parse_event(event_value("GET /create_account", user_id=str(2**63 - 1)))["user_id"], 2**63 - 1)
        self.assertEqual(parse_event(event_value("GET /create_account", user_id="00042"))["user_id"], 42)

    def test_unknown_event_preserves_all_commas_after_envelope(self):
        body = "GET /future/action,first,second"
        parsed = parse_event(event_value(body))
        self.assertEqual(parsed["parse_status"], "unrecognized")
        self.assertIsNone(parsed["event_type"])
        self.assertIsNone(parsed["error_type"])
        self.assertEqual(parsed["fields"], {"body": body})

    def test_get_query_fields_preserve_order_and_blanks_without_changing_grammar(self):
        body = "GET /future/action?tag=first&tag=second&empty="
        parsed = parse_event(event_value(body))
        self.assertEqual(parsed["parse_status"], "unrecognized")
        self.assertEqual(parsed["fields"], {
            "body": body,
            "query": {"tag": ["first", "second"], "empty": [""]},
        })

    def test_tombstone_utf8_and_invalid_envelope_are_retained_as_failures(self):
        for value, category in ((None, "Tombstone"), (b"\xff", "UnicodeDecodeError"), (b"missing,envelope", "InvalidEnvelope"), (b"", "InvalidEnvelope")):
            with self.subTest(value=value):
                parsed = parse_event(value)
                self.assertEqual(parsed["parse_status"], "failed")
                self.assertEqual(parsed["error_type"], category)
                self.assertIsNone(parsed["event_type"])
                self.assertIsNone(parsed["user_id"])
                json.dumps(parsed, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
