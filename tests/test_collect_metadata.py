import io
import json
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import unquote


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from collect_metadata import main
from services.metadata import MetadataBatch
from storage.metadata import MetadataStore


class FakeFetcher:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.closed = False

    def __call__(self, kind, ids):
        self.calls.append((kind, ids))
        if isinstance(self.response, Exception):
            raise self.response
        if self.response is not None:
            return self.response
        records = {entity_id: {
            "user_id" if kind == "user" else "id": int(entity_id) if kind == "user" else entity_id,
            "self_description_likes": None, "extra": [1, {"source": "course"}],
        } for entity_id in ids}
        return MetadataBatch(200, records, {"body": list(records.values())})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True


class CollectMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"
        self.fetchers = []
        self.response = None
        self.factory = Mock(side_effect=self.new_fetcher)

    def tearDown(self):
        self.temporary.cleanup()

    def new_fetcher(self, *_args, **_kwargs):
        fetcher = FakeFetcher(self.response)
        self.fetchers.append(fetcher)
        return fetcher

    def run_cli(self, arguments, environment=None):
        output = io.StringIO()
        errors = io.StringIO()
        with patch.dict("os.environ", environment or {}, clear=True), \
                patch("collect_metadata.MetadataHttpFetcher", self.factory), \
                patch("collect_metadata.logging.basicConfig"), \
                redirect_stdout(output), redirect_stderr(errors):
            status = main(arguments)
        payload = json.loads(output.getvalue()) if output.getvalue() else None
        return status, payload, errors.getvalue()

    def arguments(self, *more):
        return ["--base-url", "http://course.example:8080", "--storage-path", str(self.path),
                "--entity-type", "user", "--min-call-interval", "0", *more]

    def test_online_fetch_is_persisted_and_reports_retrievable_snapshot_references(self):
        status, summary, _ = self.run_cli(self.arguments("--ids", "042", "43"))
        self.assertEqual(status, 0)
        self.assertEqual(summary["fetched"], 2)
        self.assertEqual(summary["cached"], 0)
        self.assertEqual(summary["unresolved"], [])
        self.assertEqual(self.fetchers[0].calls, [("user", ["42", "43"])])
        self.assertTrue(self.fetchers[0].closed)
        with MetadataStore(self.path) as store:
            fetch = store.get_fetch(summary["fetch_ids"][0])
            self.assertEqual(fetch["source_id"], "cmu-metadata-api")
            snapshot = store.get_snapshot(summary["snapshots"]["42"]["snapshot_id"])
            self.assertIsNone(snapshot["record"]["self_description_likes"])
            self.assertEqual(snapshot["record"]["extra"], [1, {"source": "course"}])
            self.assertEqual(summary["snapshots"]["42"]["content_version"], snapshot["content_version"])

    def test_cached_and_offline_reads_do_not_fetch_and_keep_snapshot_identity(self):
        arguments = self.arguments("--ids", "42")
        _, original, _ = self.run_cli(arguments)
        status, cached, _ = self.run_cli(arguments)
        self.assertEqual(status, 0)
        self.assertEqual(cached["cached"], 1)
        self.assertEqual(cached["fetch_ids"], [])
        self.assertEqual(cached["snapshots"], original["snapshots"])
        self.assertEqual(self.fetchers[-1].calls, [])
        self.factory.reset_mock()
        status, offline, _ = self.run_cli([
            "--storage-path", str(self.path), "--entity-type", "user", "--ids", "42", "--offline",
        ])
        self.assertEqual(status, 0)
        self.assertEqual(offline["snapshots"], original["snapshots"])
        self.factory.assert_not_called()

    def test_force_refresh_keeps_earlier_observation(self):
        arguments = self.arguments("--ids", "42")
        _, original, _ = self.run_cli(arguments)
        status, refreshed, _ = self.run_cli(arguments + ["--force"])
        self.assertEqual(status, 0)
        self.assertEqual(refreshed["fetched"], 1)
        self.assertNotEqual(original["snapshots"]["42"]["snapshot_id"], refreshed["snapshots"]["42"]["snapshot_id"])
        with MetadataStore(self.path) as store:
            self.assertIsNotNone(store.get_snapshot(original["snapshots"]["42"]["snapshot_id"]))

    def test_cli_batches_inputs_and_deduplicates_before_fetching(self):
        ids = [str(value) for value in range(1, 402)] + ["001", "401"]
        status, summary, _ = self.run_cli(self.arguments("--ids", *ids))
        self.assertEqual(status, 0)
        self.assertEqual(summary["fetched"], 401)
        self.assertEqual([len(ids) for _, ids in self.fetchers[0].calls], [200, 200, 1])

    def test_missing_records_and_offline_misses_return_nonzero(self):
        self.response = MetadataBatch(200, {"42": {"user_id": 42}}, {"missing": [43]})
        status, partial, _ = self.run_cli(self.arguments("--ids", "42", "43"))
        self.assertEqual(status, 1)
        self.assertEqual(partial["fetched"], 1)
        self.assertEqual(partial["unresolved"], ["43"])
        status, offline, _ = self.run_cli(self.arguments("--ids", "42", "43", "--offline"))
        self.assertEqual(status, 1)
        self.assertEqual(offline["cached"], 1)
        self.assertEqual(offline["unresolved"], ["43"])

    def test_transport_failure_is_saved_with_safe_category_and_client_is_closed(self):
        self.response = TimeoutError("credential-sentinel")
        with self.assertLogs("services.metadata", level="WARNING") as logs:
            status, summary, errors = self.run_cli(self.arguments("--ids", "42"))
        self.assertEqual(status, 1)
        self.assertEqual(summary["unresolved"], ["42"])
        self.assertTrue(self.fetchers[0].closed)
        with MetadataStore(self.path) as store:
            fetch = store.get_fetch(summary["fetch_ids"][0])
            self.assertEqual(fetch["error_type"], "TimeoutError")
            self.assertEqual(fetch["status"], "failed")
        self.assertNotIn("credential-sentinel", errors + str(logs.output) + str(fetch))

    def test_environment_settings_configure_online_fetching(self):
        status, summary, _ = self.run_cli(
            ["--entity-type", "movie", "--ids", "american+beauty+1999"],
            {"METADATA_API_BASE_URL": "http://course.example:8080", "METADATA_SOURCE_ID": "course-data",
             "STORAGE_PATH": str(self.path)},
        )
        self.assertEqual(status, 0)
        self.assertEqual(summary["entity_type"], "movie")
        self.assertEqual(self.factory.call_args.args[0], "http://course.example:8080")
        self.assertEqual(self.fetchers[0].calls, [("movie", ["american+beauty+1999"])])
        with MetadataStore(self.path) as store:
            self.assertEqual(store.get_fetch(summary["fetch_ids"][0])["source_id"], "course-data")

    def test_invalid_cli_configuration_does_not_open_client_or_database(self):
        cases = [
            ["--entity-type", "user", "--ids", "42"],
            self.arguments("--ids", "42", "--batch-size", "201"),
            self.arguments("--ids", "42", "--timeout", "nan"),
            self.arguments("--ids", "42", "--max-response-bytes", "0"),
            self.arguments("--ids", "42", "--min-call-interval", "-1"),
            self.arguments("--ids", "42", "--force", "--offline"),
        ]
        for arguments in cases:
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit) as result:
                self.run_cli(arguments)
            self.assertEqual(result.exception.code, 2)
        self.factory.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_storage_failure_does_not_print_exception_message(self):
        with patch("collect_metadata.MetadataStore", side_effect=PermissionError("credential-sentinel")), \
                self.assertLogs("collect_metadata", level="ERROR") as logs:
            status, summary, errors = self.run_cli(self.arguments("--ids", "42"))
        self.assertEqual(status, 1)
        self.assertIsNone(summary)
        self.assertTrue(self.fetchers[0].closed)
        self.assertIn("PermissionError", str(logs.output))
        self.assertNotIn("credential-sentinel", str(logs.output) + errors)

    def test_real_http_collection_preserves_source_records_and_reuses_cache(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                requests.append((self.path, self.headers.get("Cookie")))
                kind, entity_id = self.path.split("/")[1:]
                entity_id = unquote(entity_id)
                if kind == "user":
                    record = {"user_id": int(entity_id), "self_description_likes": None,
                              "self_description_dislikes": None, "age": 29}
                else:
                    record = {"id": entity_id, "genres": [{"id": 18, "name": "Drama"}],
                              "license_cost": 0.235, "unknown": {"nested": True}}
                payload = json.dumps(record).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Set-Cookie", "session=credential-sentinel")
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_):
                pass

        def invoke(kind, ids, *more):
            output = io.StringIO()
            with patch.dict("os.environ", {"METADATA_SOURCE_ID": "cmu-metadata-api", "STORAGE_BUSY_TIMEOUT": "1"}), \
                    patch("collect_metadata.logging.basicConfig"), redirect_stdout(output):
                status = main([
                    "--base-url", f"http://127.0.0.1:{server.server_port}",
                    "--storage-path", str(self.path), "--entity-type", kind,
                    "--batch-size", "1", "--min-call-interval", "0", "--ids", *ids, *more,
                ])
            return status, json.loads(output.getvalue()) if output.getvalue() else None

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            status, users = invoke("user", ["42", "43"])
            self.assertEqual(status, 0)
            status, movie = invoke("movie", ["american+beauty+1999"])
            self.assertEqual(status, 0)
            status, cached = invoke("user", ["42", "43"])
            self.assertEqual(status, 0)
            self.assertEqual(cached["cached"], 2)
            self.assertEqual(cached["snapshots"], users["snapshots"])
            self.assertEqual(requests, [
                ("/user/42", None), ("/user/43", None),
                ("/movie/american%2Bbeauty%2B1999", None),
            ])
            with MetadataStore(self.path) as store:
                snapshot = store.get_snapshot(movie["snapshots"]["american+beauty+1999"]["snapshot_id"])
                self.assertEqual(snapshot["record"]["genres"], [{"id": 18, "name": "Drama"}])
                fetch = store.get_fetch(users["fetch_ids"][0])
                self.assertEqual(fetch["response"]["body"]["user_id"], 42)
                self.assertIsNone(fetch["response"]["body"]["self_description_likes"])
                self.assertNotIn("credential-sentinel", json.dumps(fetch))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
