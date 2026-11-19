import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from evaluate_online import sha256
from events.parser import parse_event
from experiments.config import active_plan
from license_experiment import freeze_fees, prepare, stage_slot, start
from routing import read_routing, write_json
from services.metadata import MetadataBatch
from storage.database import open_database
from storage.events import EventStore, KafkaEnvelope


@pytest.fixture
def prepared(tmp_path):
    models = tmp_path / "models"
    for slot in ("blue", "green"):
        root = models / "slots" / slot
        root.mkdir(parents=True)
        (root / "recommender.npz").write_bytes(slot.encode())
        write_json(root / "artifact-manifest.json", {"model": {"sha256": sha256(root / "recommender.npz")}})
    write_json(models / "routing.json", {"primary": "blue", "secondary_percent": 0, "salt": "test", "generation": "before"})
    fees = tmp_path / "fees.csv"
    fees.write_text("movie_id,license_cost\ncheap,0.10\nexpensive,0.80\nunknown,\ninvalid,-1\n")
    digest = sha256(models / "slots/blue/recommender.npz")
    with patch("license_experiment.get_json", return_value={"versions": {"model": "sha256:" + digest}}):
        spec = prepare(models, "license-test", fees, tmp_path / "unused", "http://metadata", refresh=False)
    return models, spec


def test_preparation_does_not_change_serving_or_register_a_run_and_staging_is_inactive_only(prepared):
    models, spec = prepared
    before = read_routing(models / "routing.json")
    assert before["secondary_percent"] == 0 and not before.get("experiment")
    assert (models / "slots/green/recommender.npz").read_bytes() == b"green"
    with pytest.raises(ValueError, match="inactive"):
        stage_slot(models, spec["id"], "blue")
    result = stage_slot(models, spec["id"], "green")
    assert sha256(models / "slots/green/recommender.npz") == spec["model_sha256"]
    assert (Path(result["backup"]) / "recommender.npz").read_bytes() == b"green"
    assert read_routing(models / "routing.json") == before
    with pytest.raises(FileExistsError):
        stage_slot(models, spec["id"], "green")


def test_start_fixes_eight_hours_same_model_and_policy_reference(prepared, tmp_path):
    models, spec = prepared
    clock = datetime(2026, 11, 20, tzinfo=timezone.utc)
    control = {"policy": "ease-with-profile-and-popularity-fallback", "top_k": 20}
    health = {
        "blue": {"versions": {"model": "sha256:" + spec["model_sha256"], "serving": "control", "dataset": "catalog"}, "ranking": control},
        "green": {"versions": {"model": "sha256:" + spec["model_sha256"], "serving": "treatment", "dataset": "catalog"},
                  "ranking": {**control, "licensing": {"penalty_per_usd": 0.01, "fees_sha256": spec["fees_sha256"]}}},
    }
    def get_health(url):
        return health["green" if "api-green" in url else "blue"]
    with patch("license_experiment.get_json", side_effect=get_health), patch(
        "license_experiment.start_experiment", return_value={"registered": True}
    ) as register:
        assert start(models, tmp_path / "capture", tmp_path / "events", spec["id"], clock=clock) == {"registered": True}
        args = register.call_args.kwargs
        assert args["start"] == clock + timedelta(minutes=3)
        assert args["enrollment_hours"] + args["follow_up_hours"] == 8
        assert args["enrollment_hours"] == 7 and args["follow_up_hours"] == 1
        assert args["expected_versions"] == {"blue": "control", "green": "treatment"}
        assert args["policy_reference"] == spec
        health["green"]["versions"]["model"] = "different"
        with pytest.raises(ValueError, match="same frozen"):
            start(models, tmp_path / "capture", tmp_path / "events", spec["id"], clock=clock)
        health["green"]["versions"]["model"] = health["blue"]["versions"]["model"]
        health["green"]["ranking"]["licensing"]["penalty_per_usd"] = 0.2
        with pytest.raises(ValueError, match="pre-specified"):
            start(models, tmp_path / "capture", tmp_path / "events", spec["id"], clock=clock)
        routing = read_routing(models / "routing.json")
        write_json(models / "routing.json", {**routing, "primary": "green"})
        with pytest.raises(ValueError, match="blue/control"):
            start(models, tmp_path / "capture", tmp_path / "events", spec["id"], clock=clock)
        assert register.call_count == 1


def test_real_registration_saves_policy_and_split_ends_at_eight_hour_boundary(prepared, tmp_path):
    models, spec = prepared
    clock = datetime(2026, 11, 20, tzinfo=timezone.utc)
    capture = tmp_path / "capture"
    capture.mkdir()
    write_json(capture / "capture-status.json", {"updated_at": clock.isoformat()})
    db = tmp_path / "events.sqlite3"
    connection = open_database(db)
    with connection:
        connection.execute("INSERT INTO live_event_cursors(source_id,topic,partition) VALUES ('cmu-movielog','movielog2',0)")
    connection.close()
    def health(url):
        if url.endswith("/health/routing"):
            return read_routing(models / "routing.json")
        if url.endswith("/health/experiment"):
            return {"healthy": True}
        slot = "green" if "api-green" in url else "blue"
        ranking = {"policy": "EASE", "top_k": 20}
        if slot == "green":
            ranking["licensing"] = {"penalty_per_usd": 0.01, "fees_sha256": spec["fees_sha256"]}
        return {"versions": {"model": "sha256:" + spec["model_sha256"],
                             "serving": slot, "dataset": "catalog"}, "ranking": ranking}
    with patch("license_experiment.get_json", side_effect=health), patch("experiment.get_json", side_effect=health):
        plan = start(models, capture, db, spec["id"], clock=clock)
    beginning = clock + timedelta(minutes=3)
    assert datetime.fromisoformat(plan["enrollment_end"]) == beginning + timedelta(hours=7)
    assert datetime.fromisoformat(plan["observation_end"]) == beginning + timedelta(hours=8)
    config = read_routing(models / "routing.json")
    assert active_plan(config, beginning - timedelta(microseconds=1)) is None
    assert active_plan(config, beginning + timedelta(hours=8) - timedelta(microseconds=1)) == plan
    assert active_plan(config, beginning + timedelta(hours=8)) is None
    saved = json.loads((models / "experiments" / spec["id"] / "plan.json").read_text())
    assert saved["ranking_policy_reference"] == spec
    assert saved["versions"] == {"blue": "blue", "green": "green"}


def test_changed_frozen_fees_cannot_be_started(prepared, tmp_path):
    models, spec = prepared
    (models / "license-policies" / spec["id"] / "fees.csv").write_text("movie_id,license_cost\ncheap,0\n")
    with patch("license_experiment.get_json", side_effect=AssertionError("must fail before network")):
        with pytest.raises(ValueError, match="assets changed"):
            start(models, tmp_path / "capture", tmp_path / "events", spec["id"])


@pytest.fixture
def fee_inputs(tmp_path):
    db = tmp_path / "events.sqlite3"
    connection = open_database(db)
    with connection:
        connection.execute("INSERT INTO live_event_cursors(source_id,topic,partition) VALUES ('cmu-movielog','movielog2',0)")
    connection.close()
    envelope = KafkaEnvelope("cmu-movielog", "movielog2", 0, 1,
                             b"2026-11-20T00:00:00Z,1,GET /data/m/new/1.mpg")
    with EventStore(db) as store:
        store.save_event(envelope, parse_event(envelope.value))
    catalog, output = tmp_path / "catalog.csv", tmp_path / "frozen.csv"
    catalog.write_text("movie_id,license_cost\nknown,0.2\nunknown,\n")
    return catalog, db, output


@pytest.mark.parametrize("reject_batch", [False, True])
def test_fee_refresh_adds_recent_movies_but_keeps_unknown_fees_unknown(fee_inputs, reject_batch):
    catalog, db, output = fee_inputs
    batch = MetadataBatch(200, records={"new": {"id": "new", "cost": 0.4},
                                      "unknown": {"id": "unknown", "cost": -1}})
    def fetch(kind, ids):
        if reject_batch and (len(ids) > 1 or ids == ["unknown"]):
            return MetadataBatch(400, error_type="HttpError")
        return MetadataBatch(200, records={movie: batch.records[movie] for movie in ids})
    with patch("license_experiment.MetadataHttpFetcher") as fetcher:
        fetcher.return_value.__enter__.return_value.side_effect = fetch
        result = freeze_fees(catalog, db, output, "http://metadata")
    with output.open() as handle:
        fees = {row["movie_id"]: row["license_cost"] for row in csv.DictReader(handle)}
    assert fees == {"known": "0.2", "new": "0.4", "unknown": ""}
    assert result["resolved_additional_fees"] == 1 and result["unknown_fees"] == 1
    assert result["rejected_movie_requests"] == ({"400": 1} if reject_batch else {})


@pytest.mark.parametrize("status", [500, None])
def test_fee_refresh_still_stops_on_server_or_transport_errors(fee_inputs, status):
    catalog, db, output = fee_inputs
    with patch("license_experiment.MetadataHttpFetcher") as fetcher:
        fetcher.return_value.__enter__.return_value.return_value = MetadataBatch(status, error_type="HttpError")
        with pytest.raises(ValueError, match="Fee preparation failed"):
            freeze_fees(catalog, db, output, "http://metadata")
    assert not output.exists()


def test_fee_refresh_budget_is_explicit_and_unresolved_prices_remain_unknown(fee_inputs, caplog):
    catalog, db, output = fee_inputs
    with patch("license_experiment.MetadataHttpFetcher") as fetcher, patch("license_experiment.MAX_FEE_REQUESTS", 1):
        fetcher.return_value.__enter__.return_value.return_value = MetadataBatch(400, error_type="HttpError")
        result = freeze_fees(catalog, db, output, "http://metadata")
    assert result["metadata_requests"] == 1 and result["deferred_movies"] == 2
    assert result["unknown_fees"] == 2 and "deferred_movies=2" in caplog.text


def test_preparation_refuses_reuse_of_saved_assets(prepared, tmp_path):
    models, spec = prepared
    fees = tmp_path / "fees.csv"
    with patch("license_experiment.get_json", return_value={"versions": {"model": "sha256:" + spec["model_sha256"]}}):
        with pytest.raises(ValueError, match="immutable"):
            prepare(models, spec["id"], fees, tmp_path / "events", "http://metadata", refresh=False)
