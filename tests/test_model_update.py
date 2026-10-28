import gzip
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataset import MovieDataset
from events.parser import parse_event
from model_update import install_candidate, verify_deployment
from prepare_recent_data import prepare_recent_data, sha256, utc
from recommender import EaseRecommender
from storage.events import EventStore, KafkaEnvelope
from storage.metadata import MetadataStore
from storage.payloads import pack_json


@pytest.fixture
def production(tmp_path):
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    movies = pd.DataFrame([
        (name, name.upper(), "Drama", "A movie", 100, 0.1)
        for name in ("a", "b", "c", "d")
    ], columns=["movie_id", "title", "genres", "overview", "runtime", "license_cost"])
    movies.to_csv(catalog / "movies.csv.gz", index=False)
    old = EaseRecommender()
    old.fit(pd.DataFrame([
        (1, "a", 1, None), (1, "b", 1, None), (2, "c", 1, None),
    ], columns=["user_id", "movie_id", "watch_count", "rating"]), movies)
    model_path = tmp_path / "recommender.npz"
    old.save(model_path)
    database = tmp_path / "events.sqlite3"
    records = [
        (0, 1, "2026-10-26T23:59:59Z", 10, "GET /rate/a=10", None),
        (0, 2, "2026-10-27T01:00:00Z", 10, "GET /data/m/a/2.mpg", None),
        (0, 3, "2026-10-27T02:00:00Z", 10, "GET /rate/b=8", None),
        (1, 1, "2026-10-27T03:00:00Z", 11, "GET /data/m/c/1.mpg", None),
        (1, 2, "2026-10-27T04:00:00Z", 12, "GET /data/m/d/1.mpg", None),
        (1, 3, "2026-10-27T05:00:00Z", 11, "GET /rate/missing=9", None),
        (1, 4, "2026-10-27T06:00:00Z", 11, "GET /rate/c=99", None),
        (1, 5, "2026-10-28T00:00:00Z", 11, "GET /rate/c=9", None),
        (1, 6, "2026-10-27T22:00:00Z", 11, "GET /rate/c=7", "2026-10-28T01:00:00Z"),
    ]
    with EventStore(database) as store:
        for partition, offset, stamp, user, body, ingested in records:
            value = f"{stamp},{user},{body}".encode()
            store.save_event(KafkaEnvelope("cmu-movielog", "movielog2", partition, offset,
                                          value, ingested_at=ingested or stamp), parse_event(value))
    with MetadataStore(database) as store:
        for name, stamp, likes in (("before", "2026-10-27T10:00:00Z", "Drama"),
                                   ("after", "2026-10-28T01:00:00Z", "Horror")):
            store.save_fetch({"fetch_id": name, "source_id": "cmu-metadata-api", "entity_type": "user",
                              "requested_ids": [10], "started_at": stamp, "finished_at": stamp,
                              "status": "success"},
                             [{"snapshot_id": name, "source_id": "cmu-metadata-api", "entity_type": "user",
                               "entity_id": 10, "fetched_at": stamp,
                               "record": {"id": 10, "self_description_likes": likes}}])
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("INSERT INTO llm_attempts VALUES(?,?,?,?,?,?,?,?)", (
            "paid", "key", "2026-10-27T12:00:00Z", None, "failed", 10, None,
            json.dumps({"model": "test-model", "endpoint": "/v1/decisions",
                        "usage": {"input_tokens": 50}, "request": {"input": "private description"}})))
        connection.commit()
    return database, catalog, model_path, tmp_path / "recent"


def prepare(production, **limits):
    database, catalog, model, output = production
    return prepare_recent_data(database, catalog, model, output,
                               start=utc("2026-10-27T00:00:00Z"), end=utc("2026-10-28T00:00:00Z"),
                               **{"max_movies": 3, **limits})


def test_recent_export_respects_window_catalog_metadata_and_existing_trainer(production):
    database, catalog, model_path, output = production
    original = sha256(database)
    report = prepare(production)
    dataset = MovieDataset(output)
    assert list(dataset.events.user_id) == [10, 10, 11]
    assert set(dataset.movies.movie_id) == {"a", "b", "c"}
    assert len(pd.read_csv(output / "catalog.csv.gz")) == 4
    assert dataset.users.set_index("user_id").loc[10, "self_description_likes"] == "Drama"
    assert dataset.users.set_index("user_id").loc[11, "self_description_likes"] == ""
    assert report["counts"]["outside_candidate_catalog"] == 2
    assert report["actual_training_start"] == "2026-10-27T01:00:00+00:00"
    assert report["actual_training_end"] == "2026-10-27T03:00:00+00:00"
    assert sha256(database) == original
    with gzip.open(output / "llm_usage.jsonl.gz", "rt") as source:
        usage = [json.loads(line) for line in source]
    assert usage[0]["usage"] == {"input_tokens": 50}
    assert "private description" not in str(usage)
    # Exercise the existing CLI on the new format, then the actual promotion path.
    candidate = output.parent / "candidate.npz"
    trained = subprocess.run([sys.executable, "src/train.py", "--data-dir", str(output),
                              "--model-path", str(candidate)], capture_output=True, text=True)
    assert trained.returncode == 0, trained.stderr
    manifest = output.parent / "manifest.json"
    installed = install_candidate(candidate, output, model_path, manifest, catalog)
    assert sha256(model_path) == sha256(candidate) != installed["previous_model_sha256"]
    assert sha256(model_path.with_name("recommender.previous.npz")) == installed["previous_model_sha256"]
    assert installed["training"]["reference_model_sha256"] == report["reference_model_sha256"]


def test_sample_caps_are_explicit_and_use_prior_movie_pool(production):
    report = prepare(production, max_events=6, max_users=1)
    dataset = MovieDataset(production[3])
    assert list(dataset.users.user_id) == [10]
    assert report["counts"]["source_rows_read"] == 6
    assert report["partitions"]["1"]["tail_capped"]
    assert not report["partitions"]["0"]["tail_capped"]


def test_empty_or_invalid_preparation_leaves_current_model_and_output_untouched(production):
    database, catalog, model, output = production
    old = model.read_bytes()
    with pytest.raises(ValueError, match="no usable"):
        prepare_recent_data(database, catalog, model, output,
                            start=utc("2027-01-01T00:00:00Z"), end=utc("2027-01-02T00:00:00Z"))
    assert not output.exists()
    assert model.read_bytes() == old
    with pytest.raises(ValueError, match="positive"):
        prepare(production, max_users=0)
    with pytest.raises(ValueError, match="timezone"):
        utc("2026-10-27T00:00:00")


@pytest.mark.parametrize("failure", ["nan", "corrupt", "changed_data", "stale_reference", "hyperparameters"])
def test_invalid_candidates_do_not_replace_deployed_artifact(production, failure):
    _, catalog, model, output = production
    prepare(production)
    dataset = MovieDataset(output)
    candidate = output.parent / "candidate.npz"
    trained = EaseRecommender(regularization=25 if failure == "hyperparameters" else 100)
    trained.fit(dataset.get_interactions(), dataset.movies)
    if failure == "nan":
        trained.weights[0, 1] = np.nan
    trained.save(candidate)
    if failure == "corrupt":
        candidate.write_bytes(b"invalid npz")
    elif failure == "changed_data":
        with (output / "catalog.csv.gz").open("ab") as destination:
            destination.write(b"changed")
    elif failure == "stale_reference":
        old = EaseRecommender.load(model)
        old.popularity[0] += 0.01
        old.save(model)
    original = model.read_bytes()
    manifest = output.parent / "manifest.json"
    manifest.write_text('{"catalog": {}}')
    with pytest.raises((ValueError, OSError)):
        install_candidate(candidate, output, model, manifest, catalog)
    assert model.read_bytes() == original
    assert manifest.read_text() == '{"catalog": {}}'
    assert not model.with_name("recommender.previous.npz").exists()


@pytest.mark.parametrize("loaded", ["candidate", "wrong-version"])
def test_deployment_check_requires_request_from_loaded_candidate(production, loaded):
    database, _, _, output = production
    manifest = output.parent / "manifest.json"
    manifest.write_text(json.dumps({"model": {"sha256": "candidate"}, "previous_model_sha256": "old",
                                    "training": {"data_directory": "recent", "actual_training_start": "start",
                                                 "actual_training_end": "end"}}))
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("INSERT INTO recommendation_requests VALUES(?,?,?,?)", (
            "request", "2026-10-28T00:00:00Z", 1,
            pack_json(json.dumps({"status": 200, "versions": {"model": "sha256:" + loaded}, "padding": "x" * 2000}))))
        connection.commit()
    response = io.BytesIO(b"a,b,c")
    response.headers = {"x-request-id": "request"}
    with patch("model_update.urlopen", return_value=response):
        if loaded == "candidate":
            assert verify_deployment(manifest, database)["recommendations"] == 3
        else:
            with pytest.raises(ValueError, match="candidate was loaded"):
                verify_deployment(manifest, database)


@pytest.mark.parametrize("failed_step", ["prepare_recent_data.py", "train.py", "model_update.py install", ""])
def test_update_script_stops_before_restart_when_a_stage_fails(tmp_path, failed_step):
    bash = shutil.which("bash")
    if os.name == "nt":
        bash = "C:/Program Files/Git/bin/bash.exe"
    if not bash or not Path(bash).is_file():
        pytest.skip("Bash is needed for the VM orchestration test")
    scripts, binaries = tmp_path / "scripts", tmp_path / "bin"
    scripts.mkdir()
    binaries.mkdir()
    shutil.copyfile("scripts/update_model.sh", scripts / "update_model.sh")
    for name, text in {
        "flock": "exit 0\n",
        "curl": "exit 0\n",
        "docker": ('printf "%s\\n" "$*" >> "$CALL_LOG"\n'
                   'if [ -n "$FAILED_STEP" ]; then\n'
                   '  case "$*" in *"$FAILED_STEP"*) exit 12 ;; esac\nfi\n'),
    }.items():
        executable = binaries / name
        executable.write_text("#!/bin/sh\n" + text, newline="\n")
        executable.chmod(0o755)
    log = tmp_path / "commands.txt"
    env = {**os.environ, "TEST_BIN": binaries.as_posix(), "UPDATE_SCRIPT": (scripts / "update_model.sh").as_posix(),
           "CALL_LOG": log.as_posix(), "FAILED_STEP": failed_step}
    result = subprocess.run([bash, "-c", 'export PATH="$(cd "$TEST_BIN" && pwd):$PATH"; exec bash "$UPDATE_SCRIPT"'],
                            env=env, capture_output=True, text=True, timeout=30)
    calls = log.read_text() if log.exists() else ""
    if failed_step:
        assert result.returncode == 12, result.stderr
        assert "force-recreate api" not in calls
    else:
        assert result.returncode == 0, result.stderr
        assert calls.index("train.py") < calls.index("model_update.py install") < calls.index("force-recreate api")
        assert "model_update.py verify" in calls
