import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from dataset import MovieDataset
from deployment import initialize_slots, slot_paths, stage_candidate, switch_traffic
from recommender import EaseRecommender
from routing import read_routing, write_json
from services.worker_lock import preparation_lock
from test_model_update import production, prepare


@pytest.fixture
def slots(production):
    _, _, model, _ = production
    from prepare_recent_data import sha256
    write_json(model.parent / "artifact-manifest.json", {"model": {"sha256": sha256(model)}})
    initialize_slots(model.parent)
    return model.parent


def test_staging_retains_active_model_and_archives_candidate(production, slots):
    _, catalog, _, data = production
    prepare(production)
    dataset = MovieDataset(data)
    trained = EaseRecommender()
    trained.fit(dataset.get_interactions(), dataset.movies)
    run = slots / "updates" / "first"
    run.mkdir(parents=True)
    candidate = run / "candidate.npz"
    trained.save(candidate)
    before = [path.read_bytes() for path in slot_paths(slots, "blue")]
    config = read_routing(slots / "routing.json")
    result = stage_candidate(slots, candidate, data, catalog)
    assert result["slot"] == "green"
    assert [path.read_bytes() for path in slot_paths(slots, "blue")] == before
    assert read_routing(slots / "routing.json") == config
    assert slot_paths(slots, "green")[0].read_bytes() == candidate.read_bytes()
    archived = json.loads((run / "artifact-manifest.json").read_text())
    assert archived["training"]["data_directory"] == str(data)
    assert archived["model"]["path"] == str(candidate)
    initialize_slots(slots)
    assert slot_paths(slots, "green")[0].read_bytes() == candidate.read_bytes()


def test_update_rejects_active_split_before_touching_either_model(slots):
    path = slots / "routing.json"
    write_json(path, {**read_routing(path), "secondary_percent": 50})
    before = [slot_paths(slots, slot)[0].read_bytes() for slot in ("blue", "green")]
    with pytest.raises(ValueError, match="traffic split"):
        stage_candidate(slots, "missing", "missing", "missing")
    assert [slot_paths(slots, slot)[0].read_bytes() for slot in ("blue", "green")] == before


def test_update_rejects_older_training_data(production, slots):
    _, catalog, _, data = production
    prepare(production)
    dataset = MovieDataset(data)
    trained = EaseRecommender()
    trained.fit(dataset.get_interactions(), dataset.movies)
    candidate = slots / "candidate.npz"
    trained.save(candidate)
    _, manifest = slot_paths(slots, "blue")
    saved = json.loads(manifest.read_text())
    write_json(manifest, {**saved, "training": {"actual_training_end": "2026-10-28T00:00:00Z"}})
    before = slot_paths(slots, "green")[0].read_bytes()
    with pytest.raises(ValueError, match="newer"):
        stage_candidate(slots, candidate, data, catalog)
    assert slot_paths(slots, "green")[0].read_bytes() == before


@pytest.mark.parametrize("failure", [None, "readiness", "public_probe"])
def test_switch_requires_readiness_and_reverts_failed_public_probe(slots, failure):
    path = slots / "routing.json"
    before = read_routing(path)
    record = slots / "switch.json"

    def get(url):
        if url.endswith("/health/routing"):
            return read_routing(path)
        if failure == "readiness":
            raise OSError("Not ready")
        manifest = slot_paths(slots, "blue")[1]
        return {"versions": {"model": "sha256:" + json.loads(manifest.read_text())["model"]["sha256"]}}

    def verify(manifest, storage, **kwargs):
        if failure == "public_probe" and "gateway" in kwargs["base_url"]:
            raise ValueError("Wrong model served")
        return {"request_id": "verified-request", "model_version": "verified-version", "http_ms": 4}

    with patch("deployment.get_json", side_effect=get), patch("model_update.verify_deployment", side_effect=verify):
        if failure:
            with pytest.raises(ValueError):
                switch_traffic(slots, "green", 0, "database", record_path=record)
            assert read_routing(path) == before
            if failure == "public_probe":
                assert json.loads(record.read_text())["status"] == "rolled_back"
        else:
            result = switch_traffic(slots, "green", 0, "database", record_path=record)
            assert result["status"] == "deployed"
            assert result["public_probes"]["green"]["request_id"] == "verified-request"
            assert read_routing(path)["primary"] == "green"


def test_profile_preparation_lock_excludes_other_process_and_releases_on_exit(tmp_path):
    database = tmp_path / "events.sqlite3"
    script = ("from services.worker_lock import preparation_lock\n"
              "import sys\n"
              "with preparation_lock(sys.argv[1]) as acquired:\n"
              "    print(acquired)\n")
    env = {**os.environ, "PYTHONPATH": str(Path("src").resolve())}

    def contender():
        result = subprocess.run([sys.executable, "-c", script, str(database)],
                                capture_output=True, text=True, env=env, timeout=10)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    with pytest.raises(RuntimeError):
        with preparation_lock(database) as acquired:
            assert acquired
            assert contender() == "False"
            raise RuntimeError("Worker failed")
    assert contender() == "True"
