import base64
import json
import logging
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import httpx


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from services.metadata import MetadataCollector, MetadataRateLimiter
from services.metadata_http import MetadataHttpFetcher
from storage.metadata import MetadataStore


class BodyStream(httpx.SyncByteStream):
    def __init__(self, chunks, error=None, close_error=None):
        self.chunks = chunks
        self.error = error
        self.close_error = close_error
        self.read = 0
        self.closed = False

    def __iter__(self):
        for chunk in self.chunks:
            self.read += 1
            yield chunk
        if self.error is not None:
            raise self.error

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class MetadataHttpTests(unittest.TestCase):
    def fetch_body(self, body, kind="user", ids=None, status=200, headers=None, **settings):
        requests = []

        def respond(request):
            requests.append(request)
            return httpx.Response(status, content=body, headers=headers)

        with MetadataHttpFetcher("http://128.2.24.239:8080", transport=httpx.MockTransport(respond), **settings) as fetcher:
            batch = fetcher(kind, ids if ids is not None else [234052])
        return batch, requests

    def test_exact_user_object_preserves_nullable_descriptions_and_raw_json(self):
        user = {
            "user_id": 234052, "age": 32, "occupation": "writer", "gender": "f",
            "self_description_likes": None, "self_description_dislikes": None,
            "extension": {"retained": [1, "🎬"]},
        }
        raw = json.dumps(user, indent=2, ensure_ascii=False).encode("utf-8")
        batch, requests = self.fetch_body(raw)
        self.assertIsNone(batch.error_type)
        self.assertEqual(batch.records, {"234052": user})
        self.assertEqual(batch.response["body"], user)
        self.assertEqual(batch.response["decoder_version"], 1)
        self.assertEqual(batch.response["raw_body"].encode("utf-8"), raw)
        self.assertEqual(batch.response["raw_body_encoding"], "utf-8")
        self.assertFalse(batch.response["body_redacted"])
        self.assertFalse(batch.response["body_truncated"])
        self.assertTrue(batch.response["body_complete"])
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(str(requests[0].url), "http://128.2.24.239:8080/user/234052")

    def test_movie_tmdb_object_keeps_nested_genres_and_complete_metadata(self):
        movie = {
            "id": "american+beauty+1999", "title": "American Beauty", "overview": "A family drama.",
            "genres": [{"id": 18, "name": "Drama"}],
            "production_companies": [{"id": 7, "name": "Studio", "logo_path": None}],
            "spoken_languages": [{"iso_639_1": "en", "name": "English"}],
            "budget": 15000000, "belongs_to_collection": None,
        }
        batch, requests = self.fetch_body(json.dumps(movie).encode(), "movie", [movie["id"]])
        self.assertIsNone(batch.error_type)
        self.assertEqual(batch.records[movie["id"]], movie)
        self.assertEqual(requests[0].url.raw_path, b"/movie/american%2Bbeauty%2B1999")

    def test_opaque_ids_are_encoded_without_escaping_base_path_prefix(self):
        movie_id = "a/../b?x=1#tail%+🎬"
        seen = []

        def respond(request):
            seen.append(request)
            return httpx.Response(200, json={"id": movie_id})

        with MetadataHttpFetcher("https://example.com/course/api/", transport=httpx.MockTransport(respond)) as fetcher:
            self.assertIsNone(fetcher("movie", [movie_id]).error_type)
        self.assertEqual(seen[0].url.raw_path, b"/course/api/movie/a%2F..%2Fb%3Fx%3D1%23tail%25%2B%F0%9F%8E%AC")
        self.assertEqual(seen[0].url.query, b"")
        self.assertEqual(seen[0].url.fragment, "")

    def test_batch_separator_is_literal_comma_and_200_ids_are_allowed(self):
        ids = [str(value) for value in range(1, 201)]
        batch, requests = self.fetch_body(json.dumps([{"user_id": int(value)} for value in ids]).encode(), ids=ids)
        self.assertIsNone(batch.error_type)
        self.assertEqual(len(batch.records), 200)
        self.assertEqual(requests[0].url.raw_path, ("/user/" + ",".join(ids)).encode())

    def test_invalid_input_never_reaches_transport(self):
        calls = []
        transport = httpx.MockTransport(lambda request: calls.append(request))
        with MetadataHttpFetcher("http://example.com", transport=transport) as fetcher:
            cases = (
                ("user", []), ("user", list(range(1, 202))), ("user", [True]),
                ("user", [0]), ("profile", [1]), ("movie", ["a,b"]),
                ("movie", ["."]), ("movie", [".."]), ("movie", [" "]),
                ("user", [42, "00042"]), ("user", "42"),
            )
            for kind, ids in cases:
                with self.subTest(kind=kind, ids=str(ids)[:40]), self.assertRaises(ValueError):
                    fetcher(kind, ids)
        self.assertEqual(calls, [])

    def test_base_urls_and_limits_are_validated(self):
        invalid_urls = (
            "ftp://example.com", "file:///tmp/data", "http:///missing-host", "http://user:password@example.com",
            "http://example.com?token=secret", "http://example.com#fragment", "http://example.com?",
            "http://example.com:", "http://example.com:0", "http://example.com:65536", "http://example.com:not-a-port",
            "http://example.com/api/../v1", "http://example.com/api/%2e%2e/v1",
            "http://example.com/api/%252e%252e/v1", "http://example.com/api/./v1",
            " http://example.com", "http://example.com/\npath", "http://example.com/api\\prefix",
        )
        for url in invalid_urls:
            with self.subTest(url=url), self.assertRaises(ValueError):
                MetadataHttpFetcher(url)
        for settings in ({"timeout": 0}, {"timeout": float("inf")}, {"timeout": True}, {"max_response_bytes": 0}, {"max_response_bytes": True}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                MetadataHttpFetcher("http://example.com", **settings)

    def test_list_and_identity_keyed_shapes_match_ids_and_allow_partial_results(self):
        cases = (
            [{"user_id": 2, "extra": "second"}, {"user_id": 1, "extra": "first"}],
            {"2": {"user_id": 2, "extra": "second"}, "1": {"user_id": 1, "extra": "first"}},
            [{"user_id": 2, "extra": "second"}],
        )
        for body in cases:
            with self.subTest(body=body):
                batch, _ = self.fetch_body(json.dumps(body).encode(), ids=[1, 2])
                self.assertIsNone(batch.error_type)
                self.assertIn("2", batch.records)
                self.assertEqual(batch.records["2"]["extra"], "second")

    def test_duplicate_conflicting_unrequested_and_missing_id_shapes_are_rejected(self):
        cases = (
            [{"user_id": 1}, {"user_id": 1}],
            [{"user_id": 1, "age": 20}, {"user_id": 1, "age": 30}],
            [{"user_id": 3}], {"1": {"user_id": 2}},
            {"1": {"user_id": 1}, "0001": {"user_id": 1}},
            [{"age": 20}], {"1": {"age": 20}},
            {"users": [{"user_id": 1}]}, ["1"], None,
            {"user_id": True},
        )
        for body in cases:
            with self.subTest(body=body):
                batch, _ = self.fetch_body(json.dumps(body).encode(), ids=[1, 2])
                self.assertEqual(batch.error_type, "InvalidMetadata")
                self.assertEqual(batch.records, {})
                self.assertEqual(batch.response["body"], body)

    def test_malformed_duplicate_key_and_nonfinite_json_are_failed_with_raw_body(self):
        cases = (
            b'{"user_id":234052', b'{"user_id":234052,"user_id":1}',
            b'{"user_id":234052,"nested":{"x":1,"x":2}}',
            b'{"user_id":234052,"number":NaN}', b'{"user_id":234052,"number":Infinity}',
            b'{"user_id":234052,"number":1e999}', b'not json',
        )
        for raw in cases:
            with self.subTest(raw=raw):
                batch, _ = self.fetch_body(raw)
                self.assertEqual(batch.error_type, "InvalidJson")
                self.assertEqual(batch.records, {})
                self.assertEqual(batch.response["raw_body"].encode(), raw)
                self.assertIsNone(batch.response["body"])

    def test_http_errors_and_redirects_preserve_body_headers_without_following(self):
        for status in (404, 429, 302):
            with self.subTest(status=status):
                batch, requests = self.fetch_body(
                    b'{"error":"unavailable","extension":[1,2]}', status=status,
                    headers=[("Retry-After", "5"), ("Location", "http://other.example.com/user/234052"), ("X-Trace", "first"), ("X-Trace", "second")],
                )
                self.assertEqual(batch.http_status, status)
                self.assertEqual(batch.error_type, "HttpError")
                self.assertEqual(batch.records, {})
                self.assertEqual(batch.response["body"]["extension"], [1, 2])
                self.assertEqual([value for name, value in batch.response["headers"] if name.lower() == "x-trace"], ["first", "second"])
                self.assertEqual(len(requests), 1)

    def test_read_timeout_preserves_partial_body_and_headers_and_closes_stream(self):
        prefix = b'{"partial":"available",'
        stream = BodyStream([prefix], httpx.ReadTimeout("token=timeout-secret"))
        transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream, headers={"x-trace": "trace-1"}))
        with MetadataHttpFetcher("http://example.com", transport=transport) as fetcher:
            batch = fetcher("user", [234052])
        self.assertEqual(batch.error_type, "ReadTimeout")
        self.assertEqual(batch.http_status, 200)
        self.assertEqual(batch.response["raw_body"].encode(), prefix)
        self.assertTrue(batch.response["body_truncated"])
        self.assertFalse(batch.response["body_complete"])
        self.assertIn(["x-trace", "trace-1"], batch.response["headers"])
        self.assertNotIn("timeout-secret", json.dumps(batch.response))
        self.assertTrue(stream.closed)

    def test_stream_cleanup_failures_retain_complete_or_partial_attempts_after_persistence(self):
        full_body = b'{"user_id":1,"unknown":{"retained":[1,2]}}'
        partial_body = b'{"user_id":1,"partial":"available",'
        cases = (
            (full_body, None, "ResponseCloseError", True),
            (partial_body, httpx.ReadTimeout("read-password=read-secret"), "ReadTimeout", False),
        )
        temporary = tempfile.TemporaryDirectory()
        try:
            with MetadataStore(Path(temporary.name) / "events.sqlite3") as store:
                for raw, read_error, expected_error, complete in cases:
                    with self.subTest(expected_error=expected_error):
                        stream = BodyStream([raw], read_error, RuntimeError("close-password=close-secret"))
                        transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream, headers={"x-trace-id": "trace-1"}))
                        with MetadataHttpFetcher("http://example.com", transport=transport) as fetcher:
                            collector = MetadataCollector(store, "course-api", fetcher, limiter=MetadataRateLimiter(0))
                            with self.assertLogs("services.metadata", level="WARNING"):
                                result = collector.collect("user", [1])
                        self.assertEqual(result.unresolved, ["1"])
                        self.assertEqual(result.fetched, 0)
                        saved = store.get_fetch(result.fetch_ids[0])
                        self.assertEqual(saved["http_status"], 200)
                        self.assertEqual(saved["error_type"], expected_error)
                        self.assertEqual(saved["response"]["close_error_type"], "ResponseCloseError")
                        self.assertEqual(saved["response"]["body_complete"], complete)
                        self.assertEqual(saved["response"]["raw_body"].encode(), raw)
                        self.assertIn(["x-trace-id", "trace-1"], saved["response"]["headers"])
                        if complete:
                            self.assertEqual(saved["response"]["body"], json.loads(raw))
                        self.assertNotIn("close-secret", json.dumps(saved))
                        self.assertNotIn("read-secret", json.dumps(saved))
                        self.assertTrue(stream.closed)
        finally:
            temporary.cleanup()

    def test_unexpected_stream_read_failure_retains_available_envelope(self):
        stream = BodyStream([b"available-prefix"], RuntimeError("password=runtime-secret"))
        with MetadataHttpFetcher(
            "http://example.com", transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream)),
        ) as fetcher:
            batch = fetcher("user", [1])
        self.assertEqual(batch.error_type, "TransportError")
        self.assertEqual(batch.http_status, 200)
        self.assertEqual(batch.response["raw_body"], "available-prefix")
        self.assertNotIn("runtime-secret", json.dumps(batch.response))
        self.assertTrue(stream.closed)

    def test_transport_timeout_has_safe_category_and_retained_request_envelope(self):
        def fail(request):
            raise httpx.ConnectTimeout("password=connection-secret", request=request)

        with MetadataHttpFetcher("http://example.com", transport=httpx.MockTransport(fail)) as fetcher:
            batch = fetcher("user", [234052])
        self.assertEqual(batch.error_type, "ConnectTimeout")
        self.assertIsNone(batch.http_status)
        self.assertEqual(batch.response["request"]["method"], "GET")
        self.assertNotIn("connection-secret", json.dumps(batch.response))

    def test_oversized_stream_stops_at_limit_and_exact_limit_is_allowed(self):
        stream = BodyStream([b"a" * 8, b"b" * 8, b"unread"])
        with MetadataHttpFetcher(
            "http://example.com", max_response_bytes=10,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream)),
        ) as fetcher:
            batch = fetcher("user", [234052])
        self.assertEqual(batch.error_type, "ResponseTooLarge")
        self.assertEqual(batch.response["raw_body"], "a" * 8 + "bb")
        self.assertTrue(batch.response["body_truncated"])
        self.assertEqual(stream.read, 2)
        self.assertTrue(stream.closed)
        raw = b'{"user_id":234052}'
        exact, _ = self.fetch_body(raw, max_response_bytes=len(raw))
        self.assertIsNone(exact.error_type)
        self.assertFalse(exact.response["body_truncated"])

    def test_total_stream_deadline_stops_trickle_response_and_retains_available_bytes(self):
        stream = BodyStream([b"first", b"second", b"unread"])
        transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
        with MetadataHttpFetcher("http://example.com", timeout=1, transport=transport) as fetcher:
            with patch("services.metadata_http.time.monotonic", side_effect=(0, 0.5, 1.5, 1.5)):
                batch = fetcher("user", [234052])
        self.assertEqual(batch.error_type, "RequestTimeout")
        self.assertEqual(batch.response["raw_body"], "firstsecond")
        self.assertEqual(stream.read, 2)
        self.assertTrue(batch.response["body_truncated"])

    def test_session_cookies_are_never_sent_on_later_requests(self):
        seen = []

        def respond(request):
            seen.append(request)
            return httpx.Response(200, json={"user_id": int(request.url.path.rsplit("/", 1)[1])}, headers={"set-cookie": "session=session-secret; Path=/"})

        with MetadataHttpFetcher("http://example.com", transport=httpx.MockTransport(respond)) as fetcher:
            self.assertIsNone(fetcher("user", [1]).error_type)
            self.assertEqual(len(fetcher.client.cookies), 0)
            self.assertIsNone(fetcher("user", [2]).error_type)
            self.assertEqual(len(fetcher.client.cookies), 0)
        self.assertEqual([request.headers.get("cookie") for request in seen], [None, None])

    def test_request_diagnostics_are_suppressed_without_muting_other_library_users(self):
        messages = []

        class Capture(logging.Handler):
            def emit(self, record):
                messages.append(record.getMessage())

        logger = logging.getLogger("httpx")
        handler = Capture()
        logger.addHandler(handler)
        previous_level = logger.level
        try:
            logger.setLevel(logging.INFO)
            self.fetch_body(b'{"user_id":234052}')
            logger.info("outside metadata adapter")
            self.assertEqual(messages, ["outside metadata adapter"])
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)

    def test_invalid_utf8_credentials_are_redacted_before_base64_fallback(self):
        raw = b"\xff\x00token=binary-secret&tag=visible"
        batch, _ = self.fetch_body(raw)
        self.assertEqual(batch.error_type, "InvalidJson")
        self.assertEqual(batch.response["raw_body_encoding"], "base64")
        retained = base64.b64decode(batch.response["raw_body"])
        self.assertNotIn(b"binary-secret", retained)
        self.assertIn(b"\xff\x00", retained)
        self.assertIn(b"visible", retained)
        self.assertTrue(batch.response["body_redacted"])

    def test_success_and_failed_envelopes_persist_sanitized_through_collector_and_store(self):
        temporary = tempfile.TemporaryDirectory()
        try:
            path = Path(temporary.name) / "events.sqlite3"
            requests = []

            def respond(request):
                requests.append(request)
                if request.url.path.endswith("/1"):
                    return httpx.Response(200, json={
                        "user_id": 1, "self_description_likes": None, "unknown": {"retained": [1, 2]},
                        "password": "body-secret", "link": "https://url-user:url-password@example.com/?token=url-secret",
                        "headers": [["Authorization", "Bearer nested-secret"]],
                    }, headers=[("Authorization", "Bearer header-secret"), ("Set-Cookie", "session=cookie-secret"), ("X-Trace", "trace-1")])
                return httpx.Response(429, content=b"\xfftoken=binary-secret&tag=visible", headers={"Retry-After": "5"})

            with MetadataStore(path) as store:
                with MetadataHttpFetcher("http://example.com", transport=httpx.MockTransport(respond)) as fetcher:
                    collector = MetadataCollector(
                        store, "course-api", fetcher, limiter=MetadataRateLimiter(0),
                        clock=lambda: datetime(2026, 10, 9, 12, tzinfo=timezone.utc),
                    )
                    success = collector.collect("user", [1])
                    self.assertEqual(success.fetched, 1)
                    self.assertIsNone(success.snapshots["1"]["record"]["self_description_likes"])
                    self.assertEqual(success.snapshots["1"]["record"]["unknown"], {"retained": [1, 2]})
                    cached = collector.collect("user", [1])
                    self.assertEqual(cached.cached, 1)
                    self.assertEqual(len(requests), 1)
                    failed = collector.collect("user", [2])
                    self.assertEqual(failed.unresolved, ["2"])
                saved_success = store.get_fetch(success.fetch_ids[0])
                saved_failure = store.get_fetch(failed.fetch_ids[0])
                encoded = json.dumps([saved_success, saved_failure, success.snapshots])
                for secret in ("body-secret", "url-user", "url-password", "url-secret", "nested-secret", "header-secret", "cookie-secret"):
                    self.assertNotIn(secret, encoded)
                self.assertEqual(saved_failure["http_status"], 429)
                self.assertEqual(saved_failure["error_type"], "HttpError")
                self.assertTrue(saved_success["response"]["body_redacted"])
                self.assertTrue(saved_success["response"]["headers_redacted"])
                retained = base64.b64decode(saved_failure["response"]["raw_body"])
                self.assertNotIn(b"binary-secret", retained)
                self.assertIn(b"visible", retained)
        finally:
            temporary.cleanup()

    def test_truncated_quoted_credentials_are_fully_redacted_after_persistence(self):
        prefixes = (
            b'{"password":"prefix-secret,tailfragment',
            b'{"password":"prefix-secret\\"quotedfragment,tailfragment',
            b'{"cookie":"prefix-secret,tailfragment\\',
        )
        temporary = tempfile.TemporaryDirectory()
        try:
            with MetadataStore(Path(temporary.name) / "events.sqlite3") as store:
                for prefix in prefixes:
                    for mode in ("size", "timeout"):
                        with self.subTest(prefix=prefix, mode=mode):
                            if mode == "size":
                                stream = BodyStream([prefix + b"unread"])
                            else:
                                stream = BodyStream([prefix], httpx.ReadTimeout("safe test error"))
                            transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
                            with MetadataHttpFetcher(
                                "http://example.com", max_response_bytes=len(prefix), transport=transport,
                            ) as fetcher:
                                collector = MetadataCollector(store, "course-api", fetcher, limiter=MetadataRateLimiter(0))
                                with self.assertLogs("services.metadata", level="WARNING"):
                                    result = collector.collect("user", [1])
                            saved = store.get_fetch(result.fetch_ids[0])
                            encoded = json.dumps(saved)
                            self.assertTrue(saved["response"]["body_truncated"])
                            self.assertTrue(saved["response"]["body_redacted"])
                            for secret in ("prefix-secret", "quotedfragment", "tailfragment"):
                                self.assertNotIn(secret, encoded)
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
