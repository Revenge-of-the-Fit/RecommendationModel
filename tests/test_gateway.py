import asyncio
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from api.gateway import create_gateway
from routing import read_routing, selected_slot, write_json


@pytest.fixture
def route(tmp_path):
    path = tmp_path / "routing.json"
    write_json(path, {"primary": "blue", "secondary_percent": 0, "salt": "test", "generation": "first"})
    return path


def response(request):
    slot = "blue" if request.url.host == "api" else "green"
    if request.url.path.startswith("/health/"):
        return httpx.Response(200, json={"status": "ready", "healthy": True})
    return httpx.Response(200, text=f"{slot}-movie,another-movie", headers={
        "x-request-id": slot + "-request", "x-model-version": "sha256:" + slot,
    })


def test_stable_user_split_serves_both_versions_and_preserves_identity(route):
    config = read_routing(route)
    config["secondary_percent"] = 50
    write_json(route, config)
    arms = set()
    with TestClient(create_gateway(route, transport=httpx.MockTransport(response))) as client:
        for user in range(1, 101):
            first, repeat = client.get(f"/recommend/{user}"), client.get(f"/recommend/{user}")
            slot = selected_slot(config, user)
            arms.add(slot)
            assert first.status_code == 200
            assert first.content == repeat.content
            assert first.headers["x-model-slot"] == first.headers["x-assigned-slot"] == slot
            assert first.headers["x-model-version"] == "sha256:" + slot
            assert first.headers["x-request-id"] == slot + "-request"
            assert first.text == f"{slot}-movie,another-movie"
            assert client.get(f"/recommend/00{user}").headers["x-assigned-slot"] == slot
    assert arms == {"blue", "green"}


def test_switch_keeps_inflight_request_on_its_loaded_version(route):
    entered, release = threading.Event(), threading.Event()

    async def backend(request):
        if request.url.host == "api":
            entered.set()
            while not release.is_set():
                await asyncio.sleep(0.001)
        return response(request)

    with TestClient(create_gateway(route, transport=httpx.MockTransport(backend))) as client:
        with ThreadPoolExecutor() as pool:
            pending = pool.submit(client.get, "/recommend/1")
            assert entered.wait(2)
            write_json(route, {**read_routing(route), "primary": "green", "generation": "second"})
            after = client.get("/recommend/2")
            release.set()
            before = pending.result(timeout=2)
    assert before.status_code == after.status_code == 200
    assert (before.headers["x-model-slot"], before.headers["x-routing-generation"]) == ("blue", "first")
    assert (after.headers["x-model-slot"], after.headers["x-routing-generation"]) == ("green", "second")


@pytest.mark.parametrize("failure", ["connection", "timeout", "invalid", "503"])
def test_unavailable_primary_falls_back_within_request_budget(route, failure):
    async def backend(request):
        if request.url.host == "api":
            if failure == "connection":
                raise httpx.ConnectError("restarting")
            if failure == "timeout":
                await asyncio.sleep(2)
            return httpx.Response(503 if failure == "503" else 200, text="invalid,invalid")
        assert request.headers["x-fast-fallback"] == "1"
        return response(request)

    with TestClient(create_gateway(route, transport=httpx.MockTransport(backend))) as client:
        started = time.monotonic()
        result = client.get("/recommend/1")
        elapsed = time.monotonic() - started
    assert result.status_code == 200
    assert result.headers["x-model-slot"] == "green"
    assert result.headers["x-assigned-slot"] == "blue"
    assert elapsed < 0.6


def test_invalid_routing_retains_last_good_configuration(route):
    with TestClient(create_gateway(route, transport=httpx.MockTransport(response))) as client:
        route.write_text('{"primary":"missing"}')
        result = client.get("/recommend/1")
    assert result.status_code == 200
    assert result.headers["x-routing-generation"] == "first"


def test_validation_and_health_failures_remain_visible(route):
    def backend(request):
        if request.url.path == "/recommend/invalid":
            return httpx.Response(422, json={"detail": "Invalid user"})
        return httpx.Response(503, json={"healthy": False})
    with TestClient(create_gateway(route, transport=httpx.MockTransport(backend))) as client:
        assert client.get("/recommend/invalid").status_code == 422
        assert client.get("/health/ready").status_code == 503
        assert client.get("/health/storage").status_code == 503
        assert client.get("/recommend/1").status_code == 503
