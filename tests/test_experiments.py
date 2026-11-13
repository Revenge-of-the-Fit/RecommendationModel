import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from api.gateway import create_gateway
from deployment import stage_candidate, switch_traffic
from events.parser import parse_event
from experiment import export_experiment, finish_experiment, start_experiment
from experiments.analysis import analyze, analyze_snapshot, markdown_report, welch
from experiments.capture import OutcomeCapture
from experiments.config import arm_for_slot, experiment_routing
from routing import read_routing, selected_slot, write_json
from storage.database import open_database
from storage.events import KafkaEnvelope
from storage.payloads import load_json
from storage.requests import RequestStore


START = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)


@pytest.fixture
def plan():
    return {"id": "fixture", "start": START.isoformat(), "enrollment_end": (START + timedelta(hours=1)).isoformat(),
            "follow_up_seconds": 3600, "observation_end": (START + timedelta(hours=2)).isoformat(),
            "salt": "fixed-test-salt", "control_slot": "blue", "treatment_percent": 50,
            "versions": {"blue": "sha256:control", "green": "sha256:treatment"},
            "source_id": "cmu-movielog", "topic": "movielog2", "partitions": [0, 1]}


def route(directory, plan=None):
    config = {"primary": "blue", "secondary_percent": 0, "salt": "before", "generation": "test"}
    if plan:
        config.update(**experiment_routing(plan), experiment=plan)
    write_json(directory / "routing.json", config)
    return directory / "routing.json"


def assigned_users(plan):
    return {arm: [user for user in range(1, 100) if arm_for_slot(plan, selected_slot(experiment_routing(plan), user)) == arm][:3]
            for arm in ("control", "treatment")}


def request(plan, user, *, seconds=0, status=200, actual=None, request_id=None):
    slot = selected_slot(experiment_routing(plan), user)
    actual = actual or slot
    return {"request_id": request_id or f"request-{user}-{seconds}", "started_at": (START + timedelta(seconds=seconds)).isoformat(),
            "user_id": user, "experiment_id": plan["id"], "assigned_slot": slot, "assigned_arm": arm_for_slot(plan, slot),
            "status": status, "actual_slot": actual if status == 200 else None,
            "served_version": plan["versions"][actual] if status == 200 else None, "backend_request_id": f"backend-{user}"}


def outcome(offset, user, movie, seconds, kind="watch", value=1, partition=0):
    return {"partition": partition, "offset": offset, "user_id": user, "movie_id": movie,
            "timestamp": (START + timedelta(seconds=seconds)).isoformat(), "kind": kind, "value": value}


def envelope(offset, seconds, kind="watch", user=1, movie="movie", value=1, partition=0):
    stamp = (START + timedelta(seconds=seconds)).isoformat()
    body = f"GET /data/m/{movie}/{value}.mpg" if kind == "watch" else f"GET /rate/{movie}={value}"
    raw = f"{stamp},{user},{body}".encode()
    return KafkaEnvelope("cmu-movielog", "movielog2", partition, offset, raw), parse_event(raw)


def test_user_level_outcomes_deduplicate_views_include_zeros_and_ignore_outside_windows(plan):
    groups = assigned_users(plan)
    a, b, zero = groups["control"]
    c, d, e = groups["treatment"]
    requests = [request(plan, user) for user in [a, b, zero, c, d, e]]
    requests += [request(plan, a, seconds=1800), request(plan, a)]
    rows = [outcome(0, a, "movie", -1), outcome(1, a, "movie", 1), outcome(2, a, "movie", 2),
            outcome(3, a, "movie", 3, "rating", 5), outcome(4, a, "movie", 4, "rating", 9),
            outcome(5, a, "other", 5, "rating", 1), outcome(6, b, "expensive", 10),
            outcome(7, c, "cheap", 10), outcome(8, c, "cheap", 11, "rating", 7),
            outcome(9, d, "cheap", 10), outcome(10, d, "cheap", 11, "rating", 8),
            outcome(11, a, "after", 3600), outcome(12, e, "cheap", 3601)]
    result = analyze(plan, requests, rows, {"movie": Decimal(2), "expensive": Decimal(4), "cheap": Decimal(1)}, set())
    control, treatment = result["arms"]["control"], result["arms"]["treatment"]
    assert control["assigned_users"] == 3 and control["observed_units"] == 2
    assert control["mean_spend_per_assigned_user_usd"] == 2
    assert treatment["mean_spend_per_assigned_user_usd"] == pytest.approx(2/3)
    assert control["mean_user_rating"] == 9 and control["rating_user_coverage"] == pytest.approx(1/3)
    assert treatment["mean_user_rating"] == 7.5
    assert result["comparisons"]["spend_per_assigned_user_usd"]["control_users"] == 3
    assert result["comparisons"]["mean_rating_per_rater"]["status"] == "insufficient_users"


def test_missing_and_invalid_fees_are_unknown_and_no_rating_is_not_zero(plan):
    user = assigned_users(plan)["control"][0]
    rows = [outcome(0, user, "missing", 1), outcome(1, user, "blank", 1), outcome(2, user, "invalid", 1)]
    result = analyze(plan, [request(plan, user)], rows, {"blank": None, "invalid": None}, {"invalid"})
    arm = result["arms"]["control"]
    assert arm["mean_spend_per_assigned_user_usd"] is None
    assert arm["mean_known_spend_per_assigned_user_usd"] == 0
    assert arm["mean_user_rating"] is None and arm["rating_users"] == 0
    assert (arm["missing_catalog_units"], arm["missing_fee_units"], arm["invalid_fee_units"]) == (1, 1, 1)
    assert result["comparisons"]["spend_per_assigned_user_usd"]["status"] == "unknown_missing_fees"
    assert "unknown" in markdown_report(result)


def test_failed_requests_and_fallback_stay_in_original_assignment(plan):
    user = assigned_users(plan)["control"][0]
    result = analyze(plan, [request(plan, user, status=503), request(plan, user, seconds=1, actual="green")],
                     [outcome(0, user, "movie", 2)], {"movie": Decimal(2)}, set())
    assert result["arms"]["control"]["assigned_users"] == 1
    assert result["arms"]["treatment"]["assigned_users"] == 0
    assert result["telemetry"] == {"requests": 2, "failed_requests": 1, "fallback_requests": 1}


def test_rating_users_get_equal_weight_and_ratings_before_watches_are_excluded(plan):
    a, b, _ = assigned_users(plan)["control"]
    rows = [outcome(0, a, "one", 2), outcome(1, a, "two", 2), outcome(2, a, "three", 2),
            outcome(3, a, "one", 3, "rating", 10), outcome(4, a, "two", 3, "rating", 10),
            outcome(5, a, "three", 1, "rating", 1), outcome(6, b, "one", 2), outcome(7, b, "one", 3, "rating", 2)]
    result = analyze(plan, [request(plan, a), request(plan, b)], rows, {}, set())
    arm = result["arms"]["control"]
    assert arm["mean_user_rating"] == 6  # (10 + 2)/2, not (10 + 10 + 2)/3.
    assert arm["mean_submitted_rating"] == pytest.approx(22/3)
    assert arm["rated_units"] == 3


def test_each_users_follow_up_starts_at_first_request_and_enrollment_end_is_exclusive(plan):
    a, b, late = assigned_users(plan)["control"]
    result = analyze(plan, [request(plan, a), request(plan, b, seconds=1800), request(plan, late, seconds=3600)],
                     [outcome(0, a, "movie", 4000), outcome(1, b, "movie", 4000), outcome(2, late, "movie", 4000)],
                     {"movie": Decimal(1)}, set())
    assert result["arms"]["control"]["assigned_users"] == 2
    assert result["arms"]["control"]["observed_units"] == 1
    assert result["arms"]["control"]["mean_spend_per_assigned_user_usd"] == 0.5


def test_welch_direction_confidence_and_unestimable_cases():
    result = welch([1, 2, 3, 4, 5], [4, 5, 6, 7, 8])
    assert result["difference"] == 3
    assert result["ci95"] == pytest.approx([0.693995865, 5.306004135])
    assert result["p_value"] == pytest.approx(0.0170716812)
    assert welch([0, 0], [0, 0])["status"] == "no_observed_variation"
    assert welch([], [1])["status"] == "insufficient_users"


def test_changed_versions_and_assignments_refuse_analysis(plan):
    row = request(plan, 1)
    with pytest.raises(ValueError, match="Served version"):
        analyze(plan, [{**row, "served_version": "changed"}], [], {}, set())
    with pytest.raises(ValueError, match="assignment"):
        analyze(plan, [{**row, "assigned_arm": "wrong"}], [], {}, set())


def test_capture_preserves_intermediate_watches_replay_and_ratings(tmp_path, plan):
    routing = route(tmp_path, plan)
    capture = OutcomeCapture(tmp_path / "capture", routing)
    events = [envelope(1, -10), envelope(2, 1), envelope(3, 2), envelope(4, 3, kind="rating", value=8), envelope(5, 8000)]
    capture.capture(events)
    capture.capture(events)
    capture.close()
    db = sqlite3.connect(tmp_path / "capture" / plan["id"] / "outcomes.sqlite3")
    try:
        assert db.execute("SELECT offset, kind, value FROM observations ORDER BY offset").fetchall() == [(2, "watch", 1), (3, "watch", 1), (4, "rating", 8)]
        assert db.execute("SELECT first_time, latest_time FROM progress").fetchone() == ((START-timedelta(seconds=10)).isoformat(), (START+timedelta(seconds=8000)).isoformat())
    finally:
        db.close()
    restarted = OutcomeCapture(tmp_path / "capture", routing)
    try:
        with pytest.raises(ValueError, match="Conflicting"):
            restarted.capture([envelope(2, 1, value=2)])
    finally:
        restarted.close()


def test_capture_refuses_a_reused_id_with_an_altered_plan(tmp_path, plan):
    routing = route(tmp_path, plan)
    capture = OutcomeCapture(tmp_path / "capture", routing)
    try:
        capture.capture([envelope(0, -1)])
        route(tmp_path, {**plan, "control_description": "Changed after registration"})
        with pytest.raises(ValueError, match="cannot change"):
            capture.capture([envelope(1, 1)])
    finally:
        capture.close()


def test_gateway_logs_normalized_assignment_actual_fallback_and_failures(tmp_path, plan):
    plan = {**plan, "start": "2020-01-01T00:00:00+00:00", "enrollment_end": "2099-01-01T00:00:00+00:00", "observation_end": "2099-01-01T01:00:00+00:00"}
    routing = route(tmp_path, plan)
    users = assigned_users(plan)
    user = users["control"][0]
    def backend(req):
        if req.url.path.endswith('/99') or req.url.host == "api":
            return httpx.Response(503)
        return httpx.Response(200, text="movie,another", headers={"x-model-version": plan["versions"]["green"], "x-request-id": "actual-backend"})
    with TestClient(create_gateway(routing, transport=httpx.MockTransport(backend), experiment_directory=tmp_path / "capture")) as client:
        assert client.get(f"/recommend/00{user}").headers["x-experiment-arm"] == "control"
        assert client.get('/recommend/99').status_code == 503
        client.app.state.experiment_log.queue.join()
        assert client.get('/health/experiment').json()["healthy"]
    with RequestStore(tmp_path / "capture" / "requests.sqlite3") as store:
        saved = store.list_requests(user)[0]
        assert saved["assigned_arm"] == "control" and saved["actual_slot"] == "green"
        assert saved["served_version"] == plan["versions"]["green"] and saved["backend_request_id"] == "actual-backend"
        failed = store.list_requests(99)[0]
        assert failed["status"] == 503 and failed["served_version"] is None


@pytest.mark.parametrize("start,end", [("2099-01-01T00:00:00Z", "2099-01-01T01:00:00Z"), ("2020-01-01T00:00:00Z", "2020-01-01T01:00:00Z")])
def test_gateway_control_only_outside_fixed_window(tmp_path, plan, start, end):
    plan.update(start=start, enrollment_end=end, observation_end=(datetime.fromisoformat(end.replace('Z', '+00:00')) + timedelta(hours=1)).isoformat())
    routing = route(tmp_path, plan)
    def backend(req):
        assert req.url.host == "api"
        return httpx.Response(200, text="movie")
    with TestClient(create_gateway(routing, transport=httpx.MockTransport(backend), experiment_directory=tmp_path / "capture")) as client:
        response = client.get(f"/recommend/{assigned_users(plan)['treatment'][0]}")
        assert response.status_code == 200 and "x-experiment-id" not in response.headers
        assert client.get('/health/experiment').json()["accepted"] == 0


def test_updates_and_manual_switches_cannot_change_registered_experiment(tmp_path, plan):
    route(tmp_path, plan)
    with pytest.raises(ValueError, match="traffic split"):
        stage_candidate(tmp_path, tmp_path / "candidate", tmp_path / "data", tmp_path / "catalog")
    with pytest.raises(ValueError, match="abort"):
        switch_traffic(tmp_path, "green", 0, tmp_path / "events")
    with pytest.raises(ValueError, match="follow-up"):
        finish_experiment(tmp_path, clock=START)
    report = finish_experiment(tmp_path, abort_reason="Fixture recovery", clock=START)
    assert report["aborted"] and read_routing(tmp_path / "routing.json")["secondary_percent"] == 0


def test_registration_capture_export_and_reanalysis_with_checksum_validation(tmp_path):
    models, capture_dir = tmp_path / "models", tmp_path / "capture"
    routing = route(models)
    capture_dir.mkdir()
    write_json(capture_dir / "capture-status.json", {"updated_at": (START-timedelta(minutes=3)).isoformat()})
    fees = tmp_path / "fees.csv"
    fees.write_text("movie_id,license_cost\nmovie,0.10\n")
    db = tmp_path / "events.sqlite3"
    connection = open_database(db)
    with connection:
        connection.executemany("INSERT INTO live_event_cursors(source_id, topic, partition) VALUES ('cmu-movielog', 'movielog2', ?)", [(0,), (1,)])
    connection.close()
    def health(url):
        if url.endswith('/health/model'):
            return {"versions": {"model": "sha256:blue" if "api:" in url else "sha256:green"}}
        if url.endswith('/health/routing'):
            return read_routing(routing)
        return {"healthy": True, "queued": 0, "experiment": read_routing(routing).get("experiment"),
                "started_at": (START-timedelta(minutes=3)).isoformat()}
    with patch('experiment.get_json', side_effect=health):
        plan = start_experiment(models, capture_dir, db, experiment_id="test-run", start=START,
            enrollment_hours=1, follow_up_hours=1, control_slot="blue", control_description="baseline",
            treatment_description="fixture variant", fees_path=fees, fee_column="license_cost", clock=START-timedelta(minutes=3))
        capture = OutcomeCapture(capture_dir, routing)
        a, b = assigned_users(plan)["control"][0], assigned_users(plan)["treatment"][0]
        capture.capture([envelope(1, -1, partition=p) for p in (0, 1)] +
                        [envelope(2, 1, user=a), envelope(3, 2, user=a, kind="rating", value=8), envelope(4, 1, user=b)] +
                        [envelope(5, 7300, partition=p) for p in (0, 1)])
        capture.close()
        with RequestStore(capture_dir / "requests.sqlite3") as store:
            store.save_request(request(plan, a))
            store.save_request(request(plan, b))
        output = models / "experiments" / plan["id"] / "snapshot"
        with pytest.raises(ValueError, match="full follow-up"):
            export_experiment(models, capture_dir, plan["id"], output, clock=START)
        for modified, message in [({"healthy": False}, "healthy"), ({"queued": 1}, "drained"),
                                  ({"started_at": (START+timedelta(minutes=1)).isoformat()}, "restarted")]:
            with patch('experiment.get_json', return_value={**health('/health/experiment'), **modified}):
                with pytest.raises(ValueError, match=message):
                    export_experiment(models, capture_dir, plan["id"], output, clock=START+timedelta(hours=3))
        connection = sqlite3.connect(capture_dir / plan["id"] / "outcomes.sqlite3")
        try:
            with connection:
                connection.execute("UPDATE progress SET latest_time=? WHERE partition=1", (START.isoformat(),))
            with pytest.raises(ValueError, match="Partition 1"):
                export_experiment(models, capture_dir, plan["id"], output, clock=START+timedelta(hours=3))
            with connection:
                connection.execute("UPDATE progress SET latest_time=? WHERE partition=1", ((START+timedelta(seconds=7300)).isoformat(),))
        finally:
            connection.close()
        result = export_experiment(models, capture_dir, plan["id"], output, clock=START+timedelta(hours=3))
        assert result["results"] == analyze_snapshot(output)
        assert result["results"]["arms"]["control"]["mean_user_rating"] == 8
        assert not finish_experiment(models, clock=START+timedelta(hours=3))["aborted"]
        (output / "fees.csv").write_text("movie_id,license_cost\nmovie,0\n")
        with pytest.raises(ValueError, match="checksum"):
            analyze_snapshot(output)
