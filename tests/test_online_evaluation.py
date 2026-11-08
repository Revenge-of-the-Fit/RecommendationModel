import contextlib
import copy
import csv
import gzip
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluate_online import evaluate_snapshot, main


SERVER = "cmu-online-mlip-vm02.andrew.cmu.edu:8082"


def event(offset, stamp, kind="rating", user=1, movie="movie-1", partition=0):
    fields = {"minute": 0} if kind == "watch" else {}
    if kind == "recommendation":
        fields = {
            "server": SERVER, "status": 200,
            "recommendations": [{"movie_id": f"movie-{rank}", "rank": rank} for rank in range(1, 21)],
        }
    return {
        "source_id": "cmu-movielog", "topic": "movielog2",
        "partition": partition, "offset": offset,
        "event_timestamp": f"2026-10-29T{stamp}+00:00",
        "ingested_at": "2026-10-29T06:00:00+00:00",
        "event_type": kind, "parse_status": "parsed",
        "user_id": user, "movie_id": movie,
        "parsed": {"fields": fields},
    }


def llm_attempt(attempt_id, *, stamp="04:00:00", status="success", input_tokens=100,
                output_tokens=20, cached_tokens=30, cache_write_tokens=5):
    return {
        "attempt_id": attempt_id, "started_at": f"2026-10-29T{stamp}+00:00",
        "finished_at": "2026-10-29T05:00:00+00:00", "status": status,
        "model": "gpt-6-luna", "endpoint": "/v1/decisions",
        "usage": {
            "input_tokens": input_tokens, "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "input_tokens_details": {"cached_tokens": cached_tokens, "cache_write_tokens": cache_write_tokens},
        },
    }


class OnlineEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / "snapshot"
        self.directory.mkdir()
        self.rows = [
            event(0, "03:00:00"),
            event(1, "04:00:00", "recommendation"),
            event(2, "04:30:00", "watch"),
            event(3, "04:00:00", "recommendation", user=2),
            event(4, "06:00:00"),
        ]
        self.arguments = {
            "start": "2026-10-29T04:00:00Z",
            "end": "2026-10-29T04:30:00Z",
            "observation_end": "2026-10-29T05:30:00Z",
            "server": SERVER,
        }
        self.catalog = [{"movie_id": "movie-1", "license_cost": "0.10"},
                        {"movie_id": "movie-2", "license_cost": "0.20"}]
        self.usage = []

    def save_snapshot(self):
        path = self.directory / "kafka_events.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as output:
            for row in self.rows:
                output.write(json.dumps(row) + "\n")
        with gzip.open(self.directory / "catalog.csv.gz", "wt", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=["movie_id", "license_cost"])
            writer.writeheader()
            writer.writerows(self.catalog)
        with gzip.open(self.directory / "llm_usage.jsonl.gz", "wt", encoding="utf-8") as output:
            for row in self.usage:
                output.write(json.dumps(row) + "\n")
        manifest = {
            "source_id": "cmu-movielog", "topic": "movielog2",
            "window_start_inclusive": "2026-10-29T03:00:00Z",
            "window_end_exclusive": "2026-10-29T06:30:00Z",
            "reference_model_sha256": "a" * 64,
            "partitions": {
                str(number): {
                    "lowest_offset": min(row["offset"] for row in self.rows if row["partition"] == number),
                    "highest_offset": max(row["offset"] for row in self.rows if row["partition"] == number),
                    "tail_capped": True,
                }
                for number in {row["partition"] for row in self.rows}
            },
            "counts": {"source_events_exported": len(self.rows), "llm_attempts_exported": len(self.usage)},
            "llm_tail_capped": True,
            "files": {
                name: hashlib.sha256((self.directory / name).read_bytes()).hexdigest()
                for name in ("kafka_events.jsonl.gz", "catalog.csv.gz", "llm_usage.jsonl.gz")
            },
        }
        (self.directory / "dataset-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def evaluate(self, **overrides):
        self.save_snapshot()
        return evaluate_snapshot(self.directory, **{**self.arguments, **overrides})

    def test_binary_response_rate_includes_users_without_later_watches(self):
        result = self.evaluate()
        self.assertEqual(result["eligible_recommendations"], 2)
        self.assertEqual(result["recommendations_with_subsequent_watch"], 1)
        self.assertEqual(result["hit_rate_at_20"], 0.5)
        self.assertEqual(result["conditional_hit_rate_at_20"], 1)
        self.assertEqual(result["recommendations_with_any_observed_watch"], 1)
        self.assertEqual(result["recommendations_without_observed_watch"], 1)
        self.assertEqual(result["recommendations_with_observed_watch_but_no_top20_match"], 0)
        self.assertEqual(result["observed_watch_rate"], 0.5)
        self.assertEqual(result["no_observed_watch_rate"], 0.5)
        self.assertEqual(result["unique_recommended_users"], 2)
        self.assertEqual(result["model"]["reference_model_sha256"], "a" * 64)
        self.assertIsNone(result["cost"]["llm_usd"])
        self.assertEqual(result["cost"]["movie_licensing_usd"], 0)
        self.assertEqual(result["cost"]["llm_tokens"], 0)

    def test_matching_requires_same_user_exact_movie_and_strictly_later_time(self):
        for stamp, user, movie in [
            ("03:59:59", 1, "movie-1"), ("04:00:00", 1, "movie-1"),
            ("05:00:00", 1, "movie-1"), ("05:00:01", 1, "movie-1"),
            ("04:30:00", 3, "movie-1"), ("04:30:00", 1, "MOVIE-1"),
            ("04:30:00", 1, "movie-21"),
        ]:
            with self.subTest(stamp=stamp, user=user, movie=movie):
                self.rows[2] = event(2, stamp, "watch", user=user, movie=movie)
                self.assertEqual(self.evaluate()["hit_rate_at_20"], 0)

    def test_rank_twenty_matches_and_rank_twenty_one_does_not(self):
        self.rows[1]["parsed"]["fields"]["recommendations"].reverse()
        self.rows[2]["movie_id"] = "movie-20"
        self.assertEqual(self.evaluate()["hit_rate_at_20"], 0.5)
        self.rows[1]["parsed"]["fields"]["recommendations"].append({"movie_id": "movie-21", "rank": 21})
        self.rows[2]["movie_id"] = "movie-21"
        self.assertEqual(self.evaluate()["hit_rate_at_20"], 0)

    def test_conditional_denominator_includes_watches_outside_top_twenty(self):
        self.rows.extend([
            event(5, "04:31:00", "watch", user=2, movie="unrecommended"),
            event(6, "04:00:00", "recommendation", user=3),
        ])
        result = self.evaluate()
        self.assertEqual(result["eligible_recommendations"], 3)
        self.assertEqual(result["recommendations_with_any_observed_watch"], 2)
        self.assertEqual(result["recommendations_without_observed_watch"], 1)
        self.assertEqual(result["recommendations_with_observed_watch_but_no_top20_match"], 1)
        self.assertEqual(result["hit_rate_at_20"], 1 / 3)
        self.assertEqual(result["conditional_hit_rate_at_20"], 0.5)
        self.assertEqual(result["observed_watch_rate"], 2 / 3)
        self.assertEqual(result["no_observed_watch_rate"], 1 / 3)

    def test_no_observed_watchers_gives_null_conditional_rate_not_zero(self):
        self.rows[2]["event_type"] = "rating"
        result = self.evaluate()
        self.assertEqual(result["hit_rate_at_20"], 0)
        self.assertIsNone(result["conditional_hit_rate_at_20"])
        self.assertEqual(result["recommendations_with_any_observed_watch"], 0)
        self.assertEqual(result["recommendations_without_observed_watch"], 2)
        self.assertEqual(result["no_observed_watch_rate"], 1)

    def test_observed_watchers_with_no_matches_give_zero_conditional_rate(self):
        self.rows[2]["movie_id"] = "unrecommended"
        result = self.evaluate()
        self.assertEqual(result["conditional_hit_rate_at_20"], 0)
        self.assertEqual(result["recommendations_with_any_observed_watch"], 1)
        self.assertEqual(result["recommendations_with_observed_watch_but_no_top20_match"], 1)

    def test_observed_watcher_cohort_uses_same_strict_time_bounds(self):
        for stamp in ("03:59:59", "04:00:00", "05:00:00", "05:00:01"):
            with self.subTest(stamp=stamp):
                self.rows[2] = event(2, stamp, "watch")
                result = self.evaluate()
                self.assertEqual(result["recommendations_with_any_observed_watch"], 0)
                self.assertIsNone(result["conditional_hit_rate_at_20"])
        self.rows[2] = event(2, "04:59:59", "watch")
        self.assertEqual(self.evaluate()["conditional_hit_rate_at_20"], 1)

    def test_duplicate_sources_impressions_and_chunks_do_not_inflate_hits(self):
        self.rows.extend([
            copy.deepcopy(self.rows[1]),
            {**copy.deepcopy(self.rows[1]), "offset": 5},
            {**copy.deepcopy(self.rows[2]), "offset": 6},
            event(7, "04:31:00", "watch"),
            event(8, "04:32:00", "watch", movie="movie-2"),
        ])
        result = self.evaluate()
        self.assertEqual(result["eligible_recommendations"], 2)
        self.assertEqual(result["recommendations_with_subsequent_watch"], 1)
        self.assertEqual(result["counts"]["duplicate_source_rows"], 1)
        self.assertEqual(result["counts"]["duplicate_impressions"], 1)
        self.assertEqual(result["counts"]["duplicate_watch_observations"], 1)
        self.assertEqual(result["recommendations_with_any_observed_watch"], 1)
        self.assertEqual(result["conditional_hit_rate_at_20"], 1)

    def test_repeated_requests_remain_distinct_and_can_share_a_watch(self):
        self.rows.append(event(5, "04:01:00", "recommendation"))
        result = self.evaluate()
        self.assertEqual(result["eligible_recommendations"], 3)
        self.assertEqual(result["recommendations_with_subsequent_watch"], 2)
        self.assertEqual(result["recommendations_with_any_observed_watch"], 2)
        self.assertEqual(result["conditional_hit_rate_at_20"], 1)

    def test_duplicate_movie_slots_are_not_refilled_from_rank_twenty_one(self):
        self.rows[1]["parsed"]["fields"]["recommendations"][19]["movie_id"] = "movie-1"
        self.rows[1]["parsed"]["fields"]["recommendations"].append({"movie_id": "movie-21", "rank": 21})
        self.rows[2]["movie_id"] = "movie-21"
        result = self.evaluate()
        self.assertEqual(result["hit_rate_at_20"], 0)
        self.assertEqual(result["counts"]["duplicate_top20_movie_slots"], 1)

    def test_recommendation_window_is_start_inclusive_end_exclusive(self):
        self.rows.extend([
            event(5, "03:59:59", "recommendation"),
            event(6, "04:30:00", "recommendation"),
        ])
        self.assertEqual(self.evaluate()["eligible_recommendations"], 2)

    def test_unsuccessful_other_server_short_and_invalid_slates_are_counted(self):
        for name, change, count in [
            ("failed", {"status": 500}, "unsuccessful_recommendations"),
            ("server", {"server": "other:8082"}, "other_server_recommendations"),
            ("short", {"recommendations": []}, "short_recommendation_slates"),
            ("invalid", {"recommendations": [{"movie_id": "movie-1", "rank": 0}]}, "invalid_recommendation_slates"),
        ]:
            with self.subTest(name=name):
                fields = self.rows[1]["parsed"]["fields"]
                saved = copy.deepcopy(fields)
                fields.update(change)
                result = self.evaluate()
                self.assertEqual(result["eligible_recommendations"], 1)
                self.assertEqual(result["hit_rate_at_20"], 0)
                self.assertEqual(result["counts"][count], 1)
                self.rows[1]["parsed"]["fields"] = saved

    def test_invalid_watch_rows_and_unparsed_rows_are_excluded_explicitly(self):
        self.rows[2]["parsed"]["fields"]["minute"] = -1
        self.rows.append({**event(5, "04:35:00", "watch"), "parse_status": "failed"})
        result = self.evaluate()
        self.assertEqual(result["hit_rate_at_20"], 0)
        self.assertEqual(result["counts"]["invalid_watch_rows"], 1)
        self.assertEqual(result["counts"]["unparsed_rows"], 1)

    def test_invalid_and_incomplete_windows_fail(self):
        for overrides in [
            {"start": "2026-10-29T04:00:00"},
            {"start": "2026-10-29T02:00:00Z"},
            {"end": self.arguments["start"]},
            {"observation_end": "2026-10-29T05:00:00Z"},
            {"observation_end": "2026-10-29T06:31:00Z"},
            {"follow_up_seconds": 0}, {"follow_up_seconds": float("nan")},
            {"follow_up_seconds": float("inf")}, {"server": ""},
        ]:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    self.evaluate(**overrides)

    def test_each_partition_must_span_the_follow_up(self):
        self.rows.append(event(0, "04:00:00", partition=1))
        with self.assertRaisesRegex(ValueError, "Partition 1 does not span"):
            self.evaluate()

    def test_timezone_offsets_match_the_same_instant(self):
        self.rows[2]["event_timestamp"] = "2026-10-29T00:30:00-04:00"
        self.assertEqual(self.evaluate()["hit_rate_at_20"], 0.5)

    def test_invalid_timestamps_and_user_ids_are_counted(self):
        self.rows[2]["event_timestamp"] = None
        self.rows.append(event(5, "04:35:00", "watch", user=0))
        result = self.evaluate()
        self.assertEqual(result["hit_rate_at_20"], 0)
        self.assertEqual(result["counts"]["invalid_timestamps"], 1)
        self.assertEqual(result["counts"]["invalid_user_rows"], 1)

    def test_later_ingestion_and_unexpected_source_fail(self):
        for field, value, message in [
            ("ingested_at", "2026-10-29T06:30:01Z", "as-of"),
            ("topic", "other-topic", "Unexpected source/topic"),
            ("offset", -1, "Invalid source coordinates"),
        ]:
            with self.subTest(field=field):
                saved = self.rows[2][field]
                self.rows[2][field] = value
                with self.assertRaisesRegex(ValueError, message):
                    self.evaluate()
                self.rows[2][field] = saved

    def test_conflicting_source_coordinates_fail(self):
        self.rows.append({**copy.deepcopy(self.rows[1]), "user_id": 3})
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            self.evaluate()

    def test_checksum_mismatch_fails(self):
        self.save_snapshot()
        with (self.directory / "kafka_events.jsonl.gz").open("ab") as output:
            output.write(b"changed")
        with self.assertRaisesRegex(ValueError, "checksum"):
            evaluate_snapshot(self.directory, **self.arguments)

    def test_missing_model_hash_and_wrong_row_count_fail(self):
        for field in ("reference_model_sha256", "counts"):
            with self.subTest(field=field):
                self.save_snapshot()
                path = self.directory / "dataset-manifest.json"
                manifest = json.loads(path.read_text())
                manifest[field] = "" if field == "reference_model_sha256" else {"source_events_exported": 99}
                path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    evaluate_snapshot(self.directory, **self.arguments)

    def test_no_eligible_recommendations_is_an_error_not_zero_accuracy(self):
        with self.assertRaisesRegex(ValueError, "No eligible"):
            self.evaluate(server="unknown:8082")

    def test_licensing_charges_once_per_user_movie_not_per_chunk_or_repeated_view(self):
        self.rows.extend([
            event(5, "04:00:00", "watch"),
            event(6, "04:01:00", "watch"),
            event(7, "04:02:00", "watch", movie="movie-2"),
            event(8, "04:03:00", "watch", user=2),
            event(9, "03:59:59", "watch", user=3),
            event(10, "04:30:00", "watch", user=3),
        ])
        self.rows[6]["parsed"]["fields"]["minute"] = 60
        result = self.evaluate()
        licensing = result["cost"]["licensing"]
        self.assertEqual(licensing["observed_units"], 3)
        self.assertEqual(licensing["covered_units"], 3)
        self.assertEqual(licensing["fee_coverage"], 1)
        self.assertEqual(result["cost"]["movie_licensing_usd"], 0.4)
        self.assertEqual(result["cost"]["movie_licensing_known_usd"], 0.4)

    def test_licensing_missing_catalog_and_missing_or_invalid_fees_are_not_free(self):
        self.rows.extend([
            event(5, "04:01:00", "watch"),
            event(6, "04:02:00", "watch", movie="unknown-movie"),
            event(7, "04:03:00", "watch", movie="movie-2"),
        ])
        for fee, counter in [
            ("", "missing_fee_units"), ("NaN", "invalid_fee_units"),
            ("Infinity", "invalid_fee_units"), ("-1", "invalid_fee_units"),
            ("invalid", "invalid_fee_units"),
        ]:
            with self.subTest(fee=fee):
                self.catalog[1]["license_cost"] = fee
                result = self.evaluate()
                licensing = result["cost"]["licensing"]
                self.assertEqual(licensing["observed_units"], 3)
                self.assertEqual(licensing["covered_units"], 1)
                self.assertEqual(licensing["missing_catalog_units"], 1)
                self.assertEqual(licensing[counter], 1)
                self.assertEqual(licensing["known_cost_usd"], 0.1)
                self.assertEqual(result["cost"]["movie_licensing_known_usd"], 0.1)
                self.assertIsNone(result["cost"]["movie_licensing_usd"])

    def test_zero_licensing_fee_is_valid_and_unmatched_watches_are_included(self):
        self.catalog.append({"movie_id": "unrecommended", "license_cost": "0"})
        self.rows.append(event(5, "04:01:00", "watch", movie="unrecommended"))
        licensing = self.evaluate()["cost"]["licensing"]
        self.assertEqual(licensing["observed_units"], 1)
        self.assertEqual(licensing["covered_units"], 1)
        self.assertEqual(licensing["total_cost_usd"], 0)

    def test_duplicate_catalog_ids_are_rejected(self):
        self.catalog.append(copy.deepcopy(self.catalog[0]))
        with self.assertRaisesRegex(ValueError, "unique"):
            self.evaluate()

    def test_llm_counts_distinct_retries_and_failed_paid_responses_once(self):
        paid = llm_attempt("first")
        self.usage = [
            paid, copy.deepcopy(paid),
            llm_attempt("retry", status="failed", input_tokens=30, output_tokens=4,
                        cached_tokens=0, cache_write_tokens=0),
            llm_attempt("before", stamp="03:59:59"),
            llm_attempt("at-end", stamp="04:30:00"),
        ]
        result = self.evaluate()
        usage = result["cost"]["llm_usage"]
        self.assertEqual(usage["attempts"], 2)
        self.assertEqual(usage["attempts_with_usage"], 2)
        self.assertEqual(usage["duplicate_export_rows"], 1)
        self.assertEqual(usage["status_counts"], {"failed": 1, "success": 1})
        self.assertEqual(usage["known_input_tokens"], 130)
        self.assertEqual(usage["known_output_tokens"], 24)
        self.assertEqual(usage["known_cached_input_tokens"], 30)
        self.assertEqual(usage["known_cache_write_tokens"], 5)
        self.assertEqual(result["cost"]["llm_tokens"], 154)
        self.assertIsNone(result["cost"]["llm_usd"])
        self.assertIsNone(usage["application_cache_reuses"])
        self.assertEqual(usage["by_model"]["gpt-6-luna"]["known_total_tokens"], 154)
        self.assertTrue(usage["export_tail_capped"])

    def test_missing_llm_usage_keeps_known_subtotal_but_not_a_complete_total(self):
        missing = llm_attempt("missing", status="failed")
        missing["usage"] = None
        self.usage = [llm_attempt("known"), missing]
        result = self.evaluate()
        usage = result["cost"]["llm_usage"]
        self.assertEqual(usage["attempts"], 2)
        self.assertEqual(usage["missing_usage_attempts"], 1)
        self.assertEqual(usage["known_total_tokens"], 120)
        self.assertIsNone(result["cost"]["llm_tokens"])

    def test_invalid_llm_usage_is_reported_as_unknown(self):
        for invalid in [
            "invalid", {"input_tokens": -1, "output_tokens": 0},
            {"input_tokens": True, "output_tokens": 0}, {"input_tokens": 10},
            {"input_tokens": 10, "output_tokens": 1, "total_tokens": 99},
        ]:
            with self.subTest(usage=invalid):
                self.usage = [{**llm_attempt("bad"), "usage": invalid}]
                result = self.evaluate()
                self.assertEqual(result["cost"]["llm_usage"]["invalid_usage_attempts"], 1)
                self.assertIsNone(result["cost"]["llm_tokens"])

    def test_missing_cache_details_do_not_discard_recorded_input_and_output(self):
        attempt = llm_attempt("missing-cache")
        del attempt["usage"]["input_tokens_details"]
        self.usage = [attempt]
        result = self.evaluate()
        self.assertEqual(result["cost"]["llm_tokens"], 120)
        self.assertEqual(result["cost"]["llm_usage"]["missing_cache_detail_attempts"], 1)

    def test_conflicting_llm_attempt_duplicates_are_rejected(self):
        self.usage = [llm_attempt("same"), llm_attempt("same", status="failed")]
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate LLM"):
            self.evaluate()

    def test_cost_exports_must_match_saved_checksums(self):
        for name in ("catalog.csv.gz", "llm_usage.jsonl.gz"):
            with self.subTest(name=name):
                self.save_snapshot()
                with (self.directory / name).open("ab") as output:
                    output.write(b"changed")
                with self.assertRaisesRegex(ValueError, "checksum"):
                    evaluate_snapshot(self.directory, **self.arguments)

    def test_cli_is_repeatable_and_does_not_modify_inputs(self):
        self.save_snapshot()
        before = {path.name: path.read_bytes() for path in self.directory.iterdir()}
        output = Path(self.temporary.name) / "results" / "online.json"
        argv = [
            "--data-dir", str(self.directory), "--output", str(output),
            "--start", self.arguments["start"], "--end", self.arguments["end"],
            "--observation-end", self.arguments["observation_end"], "--server", SERVER,
        ]
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(main(argv), 0)
            first = output.read_bytes()
        summary = json.loads(stdout.getvalue())
        self.assertEqual(summary["hit_rate_at_20"], 0.5)
        self.assertEqual(summary["conditional_hit_rate_at_20"], 1)
        self.assertEqual(summary["recommendations_without_observed_watch"], 1)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(argv), 0)
        self.assertEqual(first, output.read_bytes())
        self.assertEqual({name: (self.directory / name).read_bytes() for name in before}, before)
        self.assertEqual(json.loads(first)["hit_rate_at_20"], 0.5)
        self.assertEqual(json.loads(first)["conditional_hit_rate_at_20"], 1)

    def test_cli_refuses_to_overwrite_inputs_and_surfaces_errors(self):
        self.save_snapshot()
        argv = [
            "--data-dir", str(self.directory),
            "--output", str(self.directory / "dataset-manifest.json"),
            "--start", self.arguments["start"], "--end", self.arguments["end"],
            "--observation-end", self.arguments["observation_end"], "--server", SERVER,
        ]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as failure:
            main(argv)
        self.assertEqual(failure.exception.code, 1)
        self.assertIn("outside the read-only snapshot", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
