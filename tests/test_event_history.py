import io
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from storage.database import StorageError, open_database
from storage.event_payloads import MAGIC, SENTINEL, pack_event, unpack_event
from storage.event_history import (DEFAULT_POLICY, GIB, activate, configure, conversion_batch,
    estimate_bytes, history_status, maintain_batch, protected_windows, prune_batch, records, utc)
from storage.events import EventStore, KafkaEnvelope
from storage.live import LiveStore, read_live_user
from storage.operations import export_records

NOW = utc("2026-11-30T12:00:00Z")


def event(offset, day="2026-11-01", body="GET /rate/movie=8", **changes):
    stamp = day + "T12:00:00+00:00"
    return KafkaEnvelope("cmu-movielog", "movielog2", changes.pop("partition", 0), offset,
        f"{stamp},42,{body}".encode(), ingested_at=stamp, **changes)


def save(store, item):
    return store.save_event(item, parse_event(item.value))


def convert(path, batch=2):
    with closing(open_database(path)) as connection:
        for _ in range(100):
            if conversion_batch(connection, batch)["conversion_complete"]:
                return
    raise AssertionError("conversion did not finish")


def project(path):
    with LiveStore(path) as live:
        while live.ingest("cmu-movielog", "movielog2"):
            pass


def ledger_matches(connection):
    rows = records(connection.execute("SELECT * FROM kafka_events"))
    compressed = [row for row in rows if row["parsed_json"] == SENTINEL]
    count, size = connection.execute("SELECT coalesce(sum(row_count),0),coalesce(sum(estimated_bytes),0) FROM event_history_days").fetchone()
    assert count == len(compressed)
    assert size == sum(estimate_bytes(row) for row in compressed)


@pytest.mark.parametrize("raw", [None, b"", b"\x00\xff\xfe", MAGIC + b"source resembling encoded bytes"])
def test_codec_is_exact_and_rejects_corruption(raw):
    parsed = '{ "text": "\u96ea", "unknown": [null,false,1.25] }'
    packed, sentinel = pack_event(raw, parsed)
    assert unpack_event(raw, parsed) == (raw, parsed)
    assert unpack_event(packed, sentinel) == (raw, parsed)
    for bad in (packed[:-1], packed + b"extra", packed[:10], b"bad"):
        with pytest.raises(ValueError):
            unpack_event(bad, sentinel)
    with pytest.raises(ValueError):
        unpack_event(packed, "@MLIP:event:99")


def test_conversion_resumes_with_live_inserts_watch_replacement_and_exact_export(tmp_path):
    path = tmp_path / "events.sqlite3"
    first = event(1, body="GET /data/m/movie/1.mpg")
    with EventStore(path) as writer:
        save(writer, first)
        save(writer, event(2))
        before = writer.get_event("cmu-movielog", "movielog2", 0, 2)
        activate(path)
        with closing(open_database(path)) as maintenance:
            assert conversion_batch(maintenance, 1)["converted"] == 1
        # The already-running writer discovers activation without a restart.
        save(writer, event(3, "2026-11-02", body="GET /data/m/movie/2.mpg"))
        save(writer, event(4, partition=1))
        ledger_matches(writer.connection)
    convert(path, 1)
    with EventStore(path) as reopened:
        assert reopened.get_event("cmu-movielog", "movielog2", 0, 2) == before
        assert not save(reopened, event(2))
        with pytest.raises(StorageError):
            save(reopened, event(2, body="GET /rate/movie=7"))
        ledger_matches(reopened.connection)
        assert history_status(reopened.connection)["conversion_complete"]
    output = io.StringIO()
    export_records(path, output, tables=["kafka_events"])
    exported = [json.loads(line) for line in output.getvalue().splitlines()]
    assert all(SENTINEL not in json.dumps(item) for item in exported)
    project(path)
    assert read_live_user(path, 42)["watched"] == {"movie"}


def test_conversion_failure_rolls_back_payload_and_checkpoint(tmp_path):
    path = tmp_path / "events.sqlite3"
    with EventStore(path) as writer:
        save(writer, event(1))
        activate(path)
        original = writer.connection.execute("SELECT raw_value,parsed_json FROM kafka_events").fetchone()
        with patch("storage.event_history.account", side_effect=RuntimeError("injected write failure")):
            with pytest.raises(RuntimeError):
                conversion_batch(writer.connection)
        assert writer.connection.execute("SELECT raw_value,parsed_json FROM kafka_events").fetchone() == original
        assert writer.connection.execute("SELECT conversion_offset FROM event_history_progress").fetchone()[0] == -1
        ledger_matches(writer.connection)
    convert(path)


def test_concurrent_watch_replacement_during_conversion_does_not_resurrect_it(tmp_path):
    path = tmp_path / "events.sqlite3"
    with EventStore(path) as writer:
        save(writer, event(1, body="GET /data/m/movie/1.mpg"))
        activate(path)
        original_pack = pack_event
        def replace_during_pack(raw, parsed):
            save(writer, event(2, "2026-11-02", body="GET /data/m/movie/2.mpg"))
            return original_pack(raw, parsed)
        with closing(open_database(path)) as maintenance, patch("storage.event_history.pack_event", side_effect=replace_during_pack):
            assert conversion_batch(maintenance)["converted"] == 0
        assert writer.get_event("cmu-movielog", "movielog2", 0, 1) is None
        ledger_matches(writer.connection)


def test_retention_keeps_recent_pins_late_arrivals_and_unprojected_rows(tmp_path):
    path = tmp_path / "events.sqlite3"
    policy = {**DEFAULT_POLICY, "history_bytes": 1}
    pins = [{"id": "evidence", "reason": "test", "start": "2026-11-02T00:00:00Z", "end": "2026-11-03T00:00:00Z"}]
    samples = [event(1), event(2, "2026-11-02"), event(3, "2026-11-20"),
               replace(event(4), ingested_at="2026-11-25T00:00:00Z"),
               replace(event(5, "2026-11-02"), ingested_at="2026-11-10T00:00:00Z"),
               event(6, "2026-11-01")]
    with EventStore(path) as store:
        for item in samples:
            save(store, item)
    project(path)
    with closing(open_database(path)) as connection, connection:
        connection.execute("UPDATE live_event_cursors SET last_offset=5")
    activate(path, policy)
    convert(path)
    configure(path, pruning=True)
    before = read_live_user(path, 42)
    with closing(open_database(path)) as connection:
        for _ in range(20):
            result = prune_batch(connection, policy, pins, now=NOW, limit=1)
            if result["state"] != "pruning":
                break
        assert result["state"] == "protected_or_scanned"
        assert [row[0] for row in connection.execute("SELECT offset FROM kafka_events ORDER BY offset")] == [2, 3, 4, 5, 6]
        ledger_matches(connection)
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert read_live_user(path, 42) == before
    with EventStore(path) as writer:
        assert not save(writer, samples[0])  # expired replay cannot refill history or requeue users
        assert not save(writer, samples[1])  # retained identity still checked
        with pytest.raises(StorageError):
            save(writer, replace(samples[1], value=samples[1].value + b"changed"))


def test_budget_and_conversion_and_pause_are_hard_gates(tmp_path):
    path = tmp_path / "events.sqlite3"
    with EventStore(path) as store:
        save(store, event(1))
    project(path)
    activate(path)
    configure(path, pruning=True)
    with closing(open_database(path)) as connection:
        assert prune_batch(connection, DEFAULT_POLICY, [], now=NOW)["state"] == "converting"
    convert(path)
    with closing(open_database(path)) as connection:
        assert prune_batch(connection, DEFAULT_POLICY, [], now=NOW)["state"] == "within_target"
    configure(path, paused=True)
    with closing(open_database(path)) as connection:
        assert maintain_batch(connection, path, tmp_path)["state"] == "paused"
    with EventStore(path) as store:
        save(store, event(2))
        ledger_matches(store.connection)  # pause doesn't disable encoding/accounting


def test_automatic_evidence_pins_and_missing_model_mount(tmp_path):
    models = tmp_path / "models"
    models.mkdir()
    with pytest.raises(FileNotFoundError):
        protected_windows(DEFAULT_POLICY, models)
    plan = {"id": "experiment", "start": "2026-11-01T00:00:00Z", "observation_end": "2026-11-02T00:00:00Z"}
    (models / "routing.json").write_text(json.dumps({"experiment": plan}))
    update = models / "updates" / "one"
    update.mkdir(parents=True)
    (update / "deployment.json").write_text(json.dumps({"status": "deployed", "switched_at": "2026-11-03T10:00:00Z"}))
    pins = protected_windows({**DEFAULT_POLICY, "submission": "2026-11-25T12:00:00Z"}, models)
    assert {pin["id"] for pin in pins} == {"submission", "experiment:experiment", "update:one"}
    assert next(pin for pin in pins if pin["id"] == "submission")["end"] == "2026-11-29T12:00:00+00:00"
    (update / "availability.json").write_text('{"usable":true}')
    assert len(protected_windows(DEFAULT_POLICY, models)) == 1


def test_conversion_yields_at_disk_and_wal_limits(tmp_path):
    path = tmp_path / "events.sqlite3"
    with EventStore(path) as store:
        save(store, event(1))
        activate(path)
        with patch("storage.event_history.shutil.disk_usage") as disk:
            disk.return_value.free = 16 * GIB - 1
            assert maintain_batch(store.connection, path, tmp_path)["state"] == "filesystem_reserve"
        assert history_status(store.connection)["converted_rows"] == 0


def test_pruning_does_not_modify_profiles_costs_requests_or_metadata(tmp_path):
    # Reuse the existing audited metadata/profile/request fixture.
    from test_watch_compaction import WatchCompactionTests
    fixture = WatchCompactionTests()
    fixture.path = tmp_path / "events.sqlite3"
    fixture.seed_profile_and_request()
    path = fixture.path
    with EventStore(path) as store:
        save(store, event(1))
    project(path)
    before = fixture.other_rows()
    before.pop("nonwatch")
    policy = {**DEFAULT_POLICY, "history_bytes": 1}
    activate(path, policy)
    convert(path)
    configure(path, pruning=True)
    with closing(open_database(path)) as connection:
        assert prune_batch(connection, policy, [], now=NOW)["expired"] == 1
    after = fixture.other_rows()
    for table, rows in before.items():
        assert after[table] == rows, table


def test_minimum_safeguards_cannot_be_lowered(tmp_path):
    for change in ({"min_days": 9}, {"min_free_bytes": 15 * GIB}, {"history_bytes": 0}):
        with pytest.raises(ValueError):
            activate(tmp_path / "events.sqlite3", {**DEFAULT_POLICY, **change})


def test_compression_releases_reusable_pages_without_a_rebuild(tmp_path):
    path = tmp_path / "events.sqlite3"
    titles = ",".join("a+long+movie+title+number+" + str(i) for i in range(20))
    with EventStore(path) as store:
        store.save_events([(item, parse_event(item.value)) for item in
            [event(i, body=f"recommendation request our-vm, status 200, result: {titles}, 10 ms") for i in range(200)]])
        original = [store.get_event("cmu-movielog", "movielog2", 0, i) for i in range(200)]
        activate(path)
    convert(path, 100)
    with EventStore(path) as store:
        assert [store.get_event("cmu-movielog", "movielog2", 0, i) for i in range(200)] == original
        status = history_status(store.connection)
        assert status["reusable_database_bytes"] > 100 * 4096
        assert status["payload_bytes_saved"] > 200 * 500
        assert store.connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_profile_reuse_after_raw_expiration_never_calls_provider_again():
    from test_profile_provenance import ProfileProvenanceTests
    fixture = ProfileProvenanceTests()
    fixture.setUp()
    try:
        with fixture.sdk() as (_, paid, _):
            saved, cached = fixture.interpreter.get_profile_record("Crime dramas", "Horror")
            assert not cached
            paid.assert_called_once()
        path = fixture.database
        with EventStore(path) as store:
            save(store, event(1))
        project(path)
        policy = {**DEFAULT_POLICY, "history_bytes": 1}
        activate(path, policy)
        convert(path)
        configure(path, pruning=True)
        with closing(open_database(path)) as connection:
            assert prune_batch(connection, policy, [], now=NOW)["expired"] == 1
        with fixture.sdk(side_effect=AssertionError("Unexpected paid call")) as (_, paid, _):
            reused, cached = fixture.new_interpreter().get_profile_record("Crime dramas", "Horror")
            assert cached and reused["profile"] == saved["profile"]
            paid.assert_not_called()
    finally:
        fixture.tearDown()


def test_cli_activation_configuration_and_worker_shutdown(tmp_path, capsys):
    from manage_event_history import main
    from scripts.storage_maintenance import run_maintenance
    import threading
    path = tmp_path / "events.sqlite3"
    models = tmp_path / "models"
    models.mkdir()
    (models / "routing.json").write_text('{}')
    with EventStore(path) as store:
        save(store, event(1))
    args = ["--storage-path", str(path), "--models", str(models)]
    assert main([*args, "activate"]) == 0
    assert main([*args, "pin", "--id", "review", "--start", "2026-11-01T00:00:00Z", "--end", "2026-11-02T00:00:00Z", "--reason", "awaiting export"]) == 0
    assert main([*args, "enable-pruning"]) == 0
    assert main([*args, "pause"]) == 0
    assert main([*args, "resume"]) == 0
    stopped = threading.Event()
    from manage_event_history import report
    def report_and_stop(*args):
        stopped.set()
        return report(*args)
    with patch("scripts.storage_maintenance.history_report", side_effect=report_and_stop):
        run_maintenance(path, tmp_path / "archives", min_free_bytes=0, backups_enabled=False,
                        model_directory=models, stop_event=stopped)
    with closing(open_database(path)) as connection:
        assert history_status(connection)["conversion_complete"]
        assert connection.execute("SELECT count(*) FROM kafka_events").fetchone()[0] == 1
    assert '"operation": "event_history"' in capsys.readouterr().out


def test_drift_and_online_observation_exports_are_identical_after_conversion(tmp_path):
    from check_data_quality import collect_snapshot
    from storage.operations import export_observations
    path = tmp_path / "events.sqlite3"
    with EventStore(path) as store:
        save(store, event(1, body="recommendation request our-vm, status 200, result: movie, 10 ms"))
        save(store, replace(event(2, body="GET /data/m/movie/1.mpg"),
                            value=b"2026-11-01T12:01:00+00:00,42,GET /data/m/movie/1.mpg"))
    def snapshot():
        result = collect_snapshot(path, tmp_path, start=utc("2026-11-01T00:00:00Z"), end=utc("2026-11-02T00:00:00Z"),
                                reference={"metadata_ids": {"user": [], "movie": []}}, fetcher=lambda *_: None)
        result.pop("metadata_observed_at")
        return result
    original = snapshot()
    before = io.StringIO()
    export_observations(path, before)
    activate(path)
    convert(path)
    after = io.StringIO()
    export_observations(path, after)
    assert after.getvalue() == before.getvalue()
    assert snapshot() == original
