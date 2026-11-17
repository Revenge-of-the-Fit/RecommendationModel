"""Serve aggregate monitoring results; each scrape reuses the latest collection."""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

from check_data_quality import write_json
from evaluate_online import utc
from monitoring import MovieFeeCache, collect_report, parser


LOGGER = logging.getLogger(__name__)


def metric_lines(report, *, max_age, clock=None):
    now = clock or datetime.now(timezone.utc)
    age = (now - utc(report["generated_at"])).total_seconds() if report else None
    fresh = age is not None and 0 <= age <= max_age
    errors = (report or {}).get("errors", {})
    drift = (report or {}).get("drift", {})
    metrics = {
        "collector_fresh": int(fresh),
        "collector_age_seconds": age,
        "collection_errors": len(errors) if report else None,
        "gateway_ready": (report or {}).get("gateway_ready"),
        "availability_ratio": (report or {}).get("availability", {}).get("ratio"),
        "availability_conservative_ratio": (report or {}).get("availability", {}).get("conservative_ratio"),
        "course_requests": (report or {}).get("availability", {}).get("requests"),
        "course_request_failures": (report or {}).get("availability", {}).get("failed"),
        "course_unattributed_requests": (report or {}).get("availability", {}).get("unattributed"),
        "online_hit_rate_at_20": (report or {}).get("quality", {}).get("hit_rate_at_20"),
        "online_evaluated_requests": (report or {}).get("quality", {}).get("impressions"),
        "licensing_cost_usd": (report or {}).get("licensing", {}).get("total_cost_usd"),
        "licensing_known_cost_usd": (report or {}).get("licensing", {}).get("known_cost_usd"),
        "licensing_fee_coverage_ratio": (report or {}).get("licensing", {}).get("fee_coverage"),
        "llm_cost_usd": (report or {}).get("llm", {}).get("estimated_cost_usd"),
        "llm_known_cost_usd": (report or {}).get("llm", {}).get("known_cost_usd"),
        "llm_unknown_attempts": (report or {}).get("llm", {}).get("unknown_attempts"),
        "drift_findings": len(drift["findings"]) if "findings" in drift else None,
        "drift_strong_findings": (sum(x["reason"] == "strong_drift" for x in drift["findings"])
                                   if "findings" in drift else None),
        "drift_max_distance": max((value for features in drift.get("distances", {}).values()
                                    for value in features.values() if value is not None), default=None),
    }
    for source, size in drift.get("samples", {}).items():
        metrics[f"drift_samples_{source}"] = size
    lines = []
    for name, value in metrics.items():
        metric = "mlip_" + name
        series = value if fresh or name.startswith("collector_") else None
        lines.extend((f"# TYPE {metric} gauge", f"{metric} {'NaN' if series is None else series}"))
    return "\n".join(lines) + "\n"


def create_app():
    @asynccontextmanager
    async def lifespan(app):
        interval = int(os.getenv("MONITORING_INTERVAL_SECONDS", "300"))
        if interval < 60:
            raise ValueError("MONITORING_INTERVAL_SECONDS must be at least 60")
        app.state.max_age = 2 * interval
        path = Path(os.getenv("MONITORING_OUTPUT", "/app/state/monitoring/latest.json"))
        settings = parser().parse_args([])
        fees_path = os.getenv("MONITORING_LICENSING_FEES")
        fee_cache = MovieFeeCache()
        if settings.event_limit < 1 or not settings.server.strip():
            raise ValueError("Choose a positive collection limit and course server name")

        def collect():
            report = collect_report(settings.storage_path, settings.catalog_dir, settings.reference,
                                    server=settings.server.strip(), limit=settings.event_limit,
                                    metadata_url=settings.metadata_url, fees=Path(fees_path) if fees_path else None,
                                    fee_cache=fee_cache)
            health_url = os.getenv("MONITORING_HEALTH_URL", "http://gateway:8082/health/ready")
            try:
                with urlopen(health_url, timeout=1) as response:
                    report["gateway_ready"] = int(response.status == 200)
            except OSError:
                report["gateway_ready"] = 0
            return report

        async def refresh():
            while True:
                try:
                    report = await asyncio.to_thread(collect)
                    app.state.report = report
                    await asyncio.to_thread(write_json, path, report)
                except Exception:
                    LOGGER.exception("Monitoring collection failed")
                await asyncio.sleep(interval)

        task = asyncio.create_task(refresh())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="Internal M3 monitoring", lifespan=lifespan)
    app.state.report = None
    app.state.max_age = 600

    @app.get("/metrics")
    def metrics():
        return PlainTextResponse(metric_lines(app.state.report, max_age=app.state.max_age),
                                 media_type="text/plain; version=0.0.4")

    @app.get("/report")
    def report():
        if app.state.report is None:
            return JSONResponse({"status": "waiting_for_collection"}, status_code=503)
        age = (datetime.now(timezone.utc) - utc(app.state.report["generated_at"])).total_seconds()
        fresh = 0 <= age <= app.state.max_age
        return JSONResponse({**app.state.report, "fresh": fresh}, status_code=200 if fresh else 503)

    return app


app = create_app()
