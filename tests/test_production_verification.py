import json
import sqlite3
import unittest

from scripts.verify_production import summarize, utc


class ProductionVerificationTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.executescript("""
            CREATE TABLE recommendation_requests(request_id TEXT PRIMARY KEY, user_id INTEGER,
                started_at TEXT, record_json TEXT);
            CREATE TABLE kafka_events(source_id TEXT, topic TEXT, partition INTEGER, offset INTEGER,
                user_id INTEGER, event_timestamp TEXT, event_type TEXT, parsed_json TEXT, raw_value BLOB,
                PRIMARY KEY(source_id, topic, partition, offset));
        """)

    def tearDown(self):
        self.connection.close()

    def api(self, request_id, user_id=1, ids=("a", "b"), method="ease"):
        record = {"status": 200, "response_complete": True,
                  "recommendations": [{"movie_id": value} for value in ids],
                  "serving_method": method, "latency_ms": 10, "versions": {"code": "test"}}
        self.connection.execute("INSERT INTO recommendation_requests VALUES(?,?,?,?)",
            (request_id, user_id, "2026-10-11T10:00:00.100+00:00", json.dumps(record)))

    def course(self, offset, user_id=1, ids=("a", "b"), status=200, server="vm.example", parsed=True):
        record = {"parse_status": "parsed" if parsed else "failed",
                  "fields": {"server": server, "status": status,
                             "recommendations": [{"movie_id": value} for value in ids]}}
        self.connection.execute("INSERT INTO kafka_events VALUES(?,?,?,?,?,?,?,?,?)",
            ("cmu-movielog", "movielog2", 0, offset, user_id, "2026-10-11T10:00:00+00:00",
             "recommendation", json.dumps(record), b""))

    def result(self):
        self.connection.commit()
        return summarize(self.connection, utc("2026-10-10T12:00:00Z"),
                         utc("2026-10-11T12:00:00Z"), {"vm.example"}, {"a", "b", "c"})

    def test_failures_stay_in_denominator_and_other_servers_are_excluded(self):
        self.api("one")
        self.api("two", user_id=2, ids=("c", "a"), method="llm_cold_start")
        self.course(1)
        self.course(2, user_id=2, ids=("c", "a"))
        self.course(3, status=503)
        self.course(4, server="other.example")
        result = self.result()
        self.assertEqual(result["counts"]["all_course_requests"], 3)
        self.assertEqual(result["counts"]["successful_course_requests"], 2)
        self.assertEqual(result["personalized_percent_of_all_requests"], 66.667)
        self.assertEqual(result["distinct_personalized_rankings"], 2)

    def test_local_probes_do_not_create_course_successes(self):
        self.api("local")
        self.assertEqual(self.result()["counts"]["successful_course_requests"], 0)

    def test_ambiguous_match_is_not_counted_as_personalized(self):
        self.api("one")
        self.api("two")
        self.course(1)
        self.assertEqual(self.result()["counts"]["confirmed_personalized_successes"], 0)

    def test_one_api_request_cannot_confirm_two_course_records(self):
        self.api("one")
        self.course(1)
        self.course(2)
        result = self.result()
        self.assertEqual(result["counts"]["successful_course_requests"], 2)
        self.assertEqual(result["counts"]["confirmed_personalized_successes"], 0)

    def test_invalid_output_and_popularity_do_not_count_as_personalized(self):
        self.api("popular", method="popularity")
        self.course(1)
        self.course(2, ids=("unknown",))
        self.course(3, ids=("a", "a"))
        self.course(4, status=503, parsed=False)
        result = self.result()
        self.assertEqual(result["counts"]["all_course_requests"], 4)
        self.assertEqual(result["counts"]["successful_course_requests"], 1)
        self.assertEqual(result["counts"]["confirmed_personalized_successes"], 0)

    def test_course_connection_failure_status_zero_stays_in_denominator(self):
        self.course(1, parsed=False)
        raw = b"2026-10-11T10:00:00,1,recommendation request vm.example:8082, status 0, result: connection refused, 1 ms"
        self.connection.execute("UPDATE kafka_events SET parsed_json=?,raw_value=?",
                                (json.dumps({"parse_status": "failed", "fields": {}}), raw))
        result = self.result()
        self.assertEqual(result["counts"]["all_course_requests"], 1)
        self.assertEqual(result["counts"]["status_0"], 1)
        self.assertEqual(result["counts"]["successful_course_requests"], 0)

    def test_naive_timestamps_and_long_windows_are_rejected(self):
        with self.assertRaises(ValueError):
            utc("2026-10-11T10:00:00")
        self.connection.commit()
        with self.assertRaises(ValueError):
            summarize(self.connection, utc("2026-10-09T12:00:00Z"),
                      utc("2026-10-11T12:00:00Z"), {"vm.example"}, {"a"})


if __name__ == "__main__":
    unittest.main()
