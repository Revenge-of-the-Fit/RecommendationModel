import json
import copy
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch
from contextlib import closing
from datetime import datetime, timezone

import httpx
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_quality import SOURCES, compare, profile, validate_metadata
from check_data_quality import collect_snapshot, check_recent_quality, main
from events.parser import parse_event
from services.metadata_http import MetadataHttpFetcher
from services.metadata import MetadataBatch, MetadataCollector, MetadataRateLimiter
from storage.metadata import MetadataStore
from storage.events import EventStore, KafkaEnvelope
from storage.requests import RequestStore


@pytest.mark.parametrize("kind,record", [
    ("user", {"age": "30", "self_description_likes": None}),
    ("user", {"age": None, "self_description_dislikes": ""}),
    ("movie", {"runtime": "0", "cost": "0.1", "genres": [{"id": 18, "name": "Drama"}]}),
    ("movie", {"runtime": None, "cost": None, "genres": []}),
    ("movie", {"genres": "Drama|Comedy", "extra": {"untouched": True}}),
])
def test_supported_incomplete_metadata_is_accepted(kind, record):
    validate_metadata(kind, record)


@pytest.mark.parametrize("kind,field,value", [
    ("user", "age", True), ("user", "age", -1), ("user", "age", 121), ("user", "age", 4.5),
    ("user", "self_description_likes", ["Drama"]), ("user", "occupation", {}),
    ("movie", "cost", -0.01), ("movie", "cost", "NaN"), ("movie", "cost", True),
    ("movie", "runtime", "inf"), ("movie", "runtime", -10), ("movie", "title", []),
    ("movie", "genres", {}), ("movie", "genres", [{"id": 18}]),
])
def test_bad_metadata_fields_are_retained_for_audit_but_not_cached(tmp_path, kind, field, value):
    identity = {"user_id": 42} if kind == "user" else {"id": "a"}
    record = {**identity, field: value}
    with pytest.raises(ValueError, match="invalid_" + field):
        validate_metadata(kind, record)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=record))
    with MetadataHttpFetcher("http://example.com", transport=transport) as fetcher, MetadataStore(tmp_path / "events.sqlite3") as store:
        result = MetadataCollector(store, "course", fetcher, limiter=MetadataRateLimiter(0)).collect(
            kind, [42] if kind == "user" else ["a"])
        assert not result.snapshots
        assert result.unresolved == (["42"] if kind == "user" else ["a"])
        audit = store.get_fetch(result.fetch_ids[0])
    assert audit["error_type"] == "ValueError"
    assert audit["response"]["body"] == record


@pytest.mark.parametrize("kind", ["user", "movie"])
def test_valid_metadata_in_a_mixed_batch_remains_usable(tmp_path, kind):
    records = ([{"user_id": 1, "self_description_likes": None}, {"user_id": 2, "self_description_likes": []}]
               if kind == "user" else [{"id": "a", "cost": None}, {"id": "b", "cost": -1}])
    ids = [1, 2] if kind == "user" else ["a", "b"]
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=records))
    with MetadataHttpFetcher("http://example.com", transport=transport) as fetcher, MetadataStore(tmp_path / "events.sqlite3") as store:
        result = MetadataCollector(store, "course", fetcher, limiter=MetadataRateLimiter(0)).collect(kind, ids)
        assert list(result.snapshots) == [str(ids[0])]
        assert result.unresolved == [str(ids[1])]
        assert store.get_fetch(result.fetch_ids[0])["status"] == "partial"


@pytest.mark.parametrize("body", ["GET /data/m/a,b/1.mpg", "GET /rate/[a]=9"])
def test_unsafe_kafka_movie_ids_are_retained_as_parse_failures(body):
    parsed = parse_event(f"2026-10-28T01:00:00Z,42,{body}".encode())
    assert parsed["parse_status"] == "failed"
    assert parsed["error_type"] == "InvalidMovieID"


def normal_rows():
    return {
        "kafka": {"parse_status": "parsed", "event_type": "recommendation", "fields": {}},
        "user_api": {"user_id": 42, "age": 25, "self_description_likes": "Drama"},
        "movie_api": {"id": "a", "runtime": 90, "cost": 0.05, "genres": ["Drama"]},
        "recommendation_api": {"user_id": 42, "status": 200, "response_body": "a,b", "latency_ms": 10, "serving_method": "ease"},
    }


def snapshot(rows=None, count=100):
    return {"format_version": 1, "window": {"selection": "fixture"}, "sources": {
        source: profile(source, [record] * count) for source, record in (rows or normal_rows()).items()}}


@pytest.mark.parametrize("source,changes", [
    ("kafka", {"event_type": "rating", "fields": {"rating": 10}}),
    ("user_api", {"age": 80, "self_description_likes": None}),
    ("movie_api", {"runtime": 400, "cost": 100, "genres": ["Horror"]}),
    ("recommendation_api", {"status": 503, "serving_method": None, "latency_ms": 900}),
])
def test_strong_shifts_identify_the_affected_source(source, changes):
    reference = snapshot()
    original = copy.deepcopy(reference)
    rows = normal_rows()
    rows[source].update(changes)
    report = compare(reference, snapshot(rows))
    assert report["status"] == "failed"
    assert {finding["source"] for finding in report["findings"]} == {source}
    assert all(finding["reason"] == "strong_drift" for finding in report["findings"])
    assert reference == original



@pytest.mark.parametrize("watch_count", [0, 4675])
def test_watch_retention_changes_do_not_look_like_stream_drift(watch_count):
    def kafka(recommendations, watches, ratings, signups):
        return profile("kafka", [
            {"parse_status": "parsed", "event_type": kind, "fields": {"rating": 8}}
            for kind, count in (("recommendation", recommendations), ("watch", watches),
                                ("rating", ratings), ("account_created", signups))
            for _ in range(count)
        ])

    reference, current = snapshot(), snapshot()
    reference["sources"]["kafka"] = kafka(1545, 2068, 1351, 36)
    current["sources"]["kafka"] = kafka(180, watch_count, 142, 3)
    original = copy.deepcopy(reference)
    report = compare(reference, current)
    assert report["status"] == "passed"
    assert report["distances"]["kafka"]["event_type_non_watch"] < 0.03
    assert current["sources"]["kafka"]["features"]["event_type"].get("watch", 0) == watch_count
    assert reference == original


def test_large_rating_shift_still_fails_when_watch_counts_change():
    reference, current = snapshot(), snapshot()
    for sample, rating, watches in ((reference, 1, 100), (current, 10, 900)):
        sample["sources"]["kafka"] = profile("kafka", [
            {"parse_status": "parsed", "event_type": "rating", "fields": {"rating": rating}}
        ] * 100 + [{"parse_status": "parsed", "event_type": "watch"}] * watches)
    report = compare(reference, current)
    assert report["status"] == "failed"
    assert report["findings"] == [{"source": "kafka", "feature": "rating", "reason": "strong_drift",
                                   "distance": 1.0, "threshold": 0.3}]


def test_normal_variation_passes_and_small_features_are_not_overinterpreted():
    reference, current = snapshot(), snapshot()
    source = "user_api"
    current["sources"][source] = profile(source, [normal_rows()[source]] * 80 + [{"age": 80}] * 20)
    assert compare(reference, current)["status"] == "passed"
    reference["sources"]["kafka"]["features"]["rating"] = {"1": 1}
    current["sources"]["kafka"]["features"]["rating"] = {"10": 1}
    assert compare(reference, current)["distances"]["kafka"]["rating"] is None
    assert compare(reference, current)["status"] == "passed"


def test_insufficient_data_and_large_schema_failure_rates_do_not_pass():
    reference, current = snapshot(), snapshot()
    current["sources"]["movie_api"] = profile("movie_api", [{"cost": -1}] * 100)
    findings = compare(reference, current)["findings"]
    assert any(item["reason"] == "schema_failure_rate" and item["source"] == "movie_api" for item in findings)
    assert current["sources"]["movie_api"]["schema_errors"] == {"invalid_cost": 100}
    small = snapshot(count=49)
    assert {item["source"] for item in compare(reference, small)["findings"]} == set(SOURCES)


@pytest.mark.parametrize("changes,reason", [
    ({"user_id": None}, "invalid_request_user"),
    ({"response_body": "a,a"}, "invalid_recommendation_response"),
    ({"response_body": ",".join(str(value) for value in range(21))}, "invalid_recommendation_response"),
])
def test_api_audit_detects_invalid_inputs_and_malformed_successful_outputs(changes, reason):
    record = {**normal_rows()["recommendation_api"], **changes}
    source = profile("recommendation_api", [record] * 100)
    assert source["schema_errors"] == {reason: 100}
    current = snapshot()
    current["sources"]["recommendation_api"] = source
    assert compare(snapshot(), current)["status"] == "failed"


@pytest.fixture
def observed_data(tmp_path):
    database = tmp_path / "events.sqlite3"
    with EventStore(database) as store:
        for index in range(100):
            value = f"2026-10-28T01:00:00Z,{index+1},GET /data/m/m000/1.mpg".encode()
            store.save_event(KafkaEnvelope("cmu-movielog", "movielog2", index % 2, index // 2,
                                          value, ingested_at="2026-10-28T01:00:01+00:00"), parse_event(value))
    with RequestStore(database) as store:
        for index in range(100):
            store.save_request({"request_id": str(index), "started_at": "2026-10-28T01:00:02Z",
                                **normal_rows()["recommendation_api"]})
    pd.DataFrame({"movie_id": [f"m{index:03}" for index in range(100)]}).to_csv(tmp_path / "movies.csv.gz", index=False)
    start = datetime(2026, 10, 28, tzinfo=timezone.utc)
    end = datetime(2026, 10, 29, tzinfo=timezone.utc)
    return database, tmp_path, start, end


def test_production_reader_is_bounded_reuses_api_decoder_and_releases_snapshot(observed_data):
    database, catalog, start, end = observed_data
    seen = []

    def respond(request):
        # The live read transaction must already be closed before waiting on an API.
        with closing(sqlite3.connect(database, timeout=0.1)) as connection:
            connection.execute("UPDATE live_event_cursors SET last_offset=last_offset")
            connection.commit()
            assert connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
        kind, ids = request.url.path.strip("/").split("/")
        seen.append((kind, ids.split(",")))
        return httpx.Response(200, json=[{
            **normal_rows()[kind + "_api"], **({"user_id": int(value)} if kind == "user" else {"id": value}),
        } for value in ids.split(",")])

    with MetadataHttpFetcher("http://example.com", transport=httpx.MockTransport(respond)) as fetcher:
        reference = collect_snapshot(database, catalog, start=start, end=end, limit=80, fetcher=fetcher)
    assert reference["sources"]["kafka"]["sample_size"] == 80
    assert reference["sources"]["recommendation_api"]["sample_size"] == 80
    assert reference["coverage"]["recommendation_tail_capped"]
    assert len(seen[0][1]) == 80 and len(seen[1][1]) == 100
    assert compare(reference, reference)["status"] == "passed"
    path = catalog / "reference.json"
    path.write_text(json.dumps(reference))

    def shifted(kind, ids):
        return MetadataBatch(200, {value: {**normal_rows()[kind + "_api"], **({"cost": 100} if kind == "movie" else {})}
                                  for value in ids})

    report = check_recent_quality(database, catalog, path, start=start, end=end, limit=80, fetcher=shifted)
    assert report["status"] == "failed"
    assert {item["source"] for item in report["findings"]} == {"movie_api"}
    assert json.loads(path.read_text()) == reference


def test_reference_creation_requires_coverage_and_does_not_silently_overwrite(tmp_path, monkeypatch):
    path = tmp_path / "reference.json"
    monkeypatch.setattr(sys, "argv", ["quality", "--save-reference", str(path)])
    with patch("check_data_quality.collect_snapshot", return_value=snapshot(count=49)):
        assert main() == 2
    assert not path.exists()
    with patch("check_data_quality.collect_snapshot", return_value=snapshot()):
        assert main() == 0
    original = path.read_bytes()
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    assert path.read_bytes() == original
