"""Keep port 8082 available while either model server is replaced."""
import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import TypeAdapter

from routing import BACKENDS, other_slot, read_routing, selected_slot
from experiments.config import active_plan, arm_for_slot
from storage.requests import RequestLog


LOGGER = logging.getLogger(__name__)
USER_ID = TypeAdapter(int)


def create_gateway(routing_path=None, *, transport=None, experiment_directory=None):
    routing_path = Path(routing_path or os.environ.get("ROUTING_PATH", "/app/models/routing.json"))
    experiment_directory = experiment_directory or os.environ.get("EXPERIMENT_DIRECTORY")

    @asynccontextmanager
    async def lifespan(app):
        app.state.routing = read_routing(routing_path)
        app.state.started_at = datetime.now(timezone.utc).isoformat()
        app.state.experiment_log = RequestLog(Path(experiment_directory) / "requests.sqlite3", busy_timeout=5) if experiment_directory else None
        try:
            async with httpx.AsyncClient(transport=transport, trust_env=False,
                                        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)) as client:
                app.state.client = client
                yield
        finally:
            if app.state.experiment_log is not None:
                app.state.experiment_log.close()

    app = FastAPI(title="Recommendation router", lifespan=lifespan)

    def configuration():
        try:
            app.state.routing = read_routing(routing_path)
        except (OSError, ValueError, KeyError, TypeError):
            LOGGER.error("Cannot read traffic configuration; keeping the last valid configuration")
        return app.state.routing

    @app.get("/health/routing")
    async def routing():
        return configuration()

    @app.get("/health/experiment")
    async def experiment_health():
        writer = app.state.experiment_log
        status = writer.status() if writer is not None else {"healthy": False, "last_error": "not_configured"}
        return JSONResponse({**status, "started_at": app.state.started_at,
                             "experiment": configuration().get("experiment")},
                            status_code=200 if status["healthy"] else 503)

    @app.get("/health/ready")
    async def readiness():
        config = configuration()
        for slot in (config["primary"], other_slot(config["primary"])):
            try:
                async with asyncio.timeout(0.25):
                    response = await app.state.client.get(BACKENDS[slot] + "/health/ready")
                if response.status_code == 200:
                    return {"status": "ready"}
            except (httpx.HTTPError, TimeoutError):
                pass
        return JSONResponse({"status": "unavailable"}, status_code=503)

    @app.get("/health/storage")
    async def storage():
        results = {}
        for slot in BACKENDS:
            try:
                async with asyncio.timeout(1):
                    response = await app.state.client.get(BACKENDS[slot] + "/health/storage")
                results[slot] = response.json()
                results[slot]["healthy"] = response.status_code == 200 and results[slot].get("healthy", False)
            except (httpx.HTTPError, TimeoutError, ValueError):
                results[slot] = {"healthy": False, "error": "unreachable"}
        healthy = all(item["healthy"] for item in results.values())
        return JSONResponse({"healthy": healthy, "slots": results}, status_code=200 if healthy else 503)

    @app.get("/recommend/{userid}")
    async def recommend(userid: str):
        config = configuration()
        stamp = datetime.now(timezone.utc)
        plan = active_plan(config, stamp)
        if config.get("experiment") and plan is None:
            config = {**config, "secondary_percent": 0}  # Only control outside the fixed run window.
        try:
            normalized = USER_ID.validate_python(userid)
        except ValueError:
            normalized = userid  # The backend records and rejects invalid input.
        assigned = selected_slot(config, normalized)
        trace = {}
        status = None
        try:
            response = await forward(userid, assigned, config, trace)
            status = response.status_code
            if plan is not None:
                response.headers["x-experiment-id"] = plan["id"]
                response.headers["x-experiment-arm"] = arm_for_slot(plan, assigned)
            return response
        finally:
            if plan is not None and type(normalized) is int and 0 < normalized <= 2**63 - 1:
                writer = app.state.experiment_log
                if writer is not None:
                    writer.submit({
                        "request_id": uuid.uuid4().hex, "started_at": stamp.isoformat(),
                        "user_id": normalized, "experiment_id": plan["id"],
                        "assigned_arm": arm_for_slot(plan, assigned), "assigned_slot": assigned,
                        "routing_generation": config["generation"], "status": status,
                        "actual_slot": trace.get("slot"), "served_version": trace.get("version"),
                        "backend_request_id": trace.get("request_id"),
                    })
                else:
                    LOGGER.error("Experiment assignment could not be recorded")

    async def forward(userid, assigned, config, trace):
        deadline = time.monotonic() + 0.55
        for attempt, slot in enumerate((assigned, other_slot(assigned))):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                async with asyncio.timeout(min(0.45, remaining)):
                    response = await app.state.client.get(
                        BACKENDS[slot] + "/recommend/" + quote(userid, safe=""),
                        headers={"x-fast-fallback": "1"} if attempt else {},
                    )
                if response.status_code == 422:
                    # Let the model API validate and record bad inputs as it did in M2.
                    return Response(response.content, status_code=422, media_type="application/json",
                                    headers={"x-request-id": response.headers.get("x-request-id", "")})
                if response.status_code != 200:
                    continue
                ids = response.text.strip().split(",")
                if not 1 <= len(ids) <= 20 or len(set(ids)) != len(ids) or any(
                    not item or any(c.isspace() for c in item) for item in ids
                ):
                    continue
                trace.update(slot=slot, version=response.headers.get("x-model-version"),
                             request_id=response.headers.get("x-request-id"))
                return Response(response.content, media_type="text/plain", headers={
                    "x-request-id": response.headers.get("x-request-id", ""),
                    "x-model-version": response.headers.get("x-model-version", ""),
                    "x-model-slot": slot,
                    "x-assigned-slot": assigned,
                    "x-routing-generation": config["generation"],
                })
            except (httpx.HTTPError, TimeoutError):
                continue
        return PlainTextResponse("Recommendations are unavailable", status_code=503)

    return app


app = create_gateway()
