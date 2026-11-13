"""Keep port 8082 available while either model server is replaced."""
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import TypeAdapter

from routing import BACKENDS, other_slot, read_routing, selected_slot


LOGGER = logging.getLogger(__name__)
USER_ID = TypeAdapter(int)


def create_gateway(routing_path=None, *, transport=None):
    routing_path = Path(routing_path or os.environ.get("ROUTING_PATH", "/app/models/routing.json"))

    @asynccontextmanager
    async def lifespan(app):
        app.state.routing = read_routing(routing_path)
        async with httpx.AsyncClient(transport=transport, trust_env=False,
                                    limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)) as client:
            app.state.client = client
            yield

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
        try:
            normalized = USER_ID.validate_python(userid)
        except ValueError:
            normalized = userid  # The backend records and rejects invalid input.
        assigned = selected_slot(config, normalized)
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
