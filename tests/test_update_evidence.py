import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from events.parser import parse_event
from prepare_recent_data import utc
from routing import write_json
from storage.events import EventStore, KafkaEnvelope
from update_evidence import collect_evidence


@pytest.fixture
def observations(tmp_path):
    database = tmp_path / "events.sqlite3"
    record = tmp_path / "deployment.json"
    write_json(record, {"status": "deployed", "switched_at": "2026-11-13T12:00:00Z"})
    samples = [
        ("11:50:00", "our-vm", 200), ("11:58:00", "our-vm", 200),
        ("11:59:00", "another-vm", 200), ("12:00:01", "our-vm", 504),
        ("12:00:02", "our-vm", 200), ("12:10:00", "our-vm", 200),
    ]
    with EventStore(database) as store:
        for offset, (stamp, server, status) in enumerate(samples):
            value = f"2026-11-13T{stamp}Z,1,recommendation request {server}, status {status}, result: movie, 10ms".encode()
            store.save_event(KafkaEnvelope("cmu-movielog", "movielog2", 0, offset, value), parse_event(value))
    return record, database


def test_evidence_counts_failures_and_only_exact_server_with_submission_window(observations):
    record, database = observations
    report = collect_evidence(record, database, "our-vm", clock=utc("2026-11-13T12:12:00Z"),
                              submission=utc("2026-11-16T11:59:00Z"))
    assert report["usable"]
    assert report["overall"]["requests"] == 3
    assert report["overall"]["availability"] == 2 / 3
    assert report["after"]["statuses"] == {"504": 1, "200": 1}
    assert report["in_final_72_hours"]
    assert (record.parent / "availability-requests.jsonl.gz").is_file()
    assert json.loads((record.parent / "availability.json").read_text())["usable"]


def test_recent_or_incomplete_observations_do_not_claim_availability(observations):
    record, database = observations
    with pytest.raises(ValueError, match="Wait until"):
        collect_evidence(record, database, "our-vm", clock=utc("2026-11-13T12:01:00Z"))
    report = collect_evidence(record, database, "our-vm", max_events=2,
                              clock=utc("2026-11-13T12:12:00Z"), submission=utc("2026-11-17T00:00:00Z"))
    assert not report["usable"]
    assert not report["window_bracketed"]
    assert not report["in_final_72_hours"]
