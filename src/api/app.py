"""Create the FastAPI application and load serving resources once at startup."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from api.routes import router
from api.request_logging import RequestLoggingMiddleware
from models.settings import ServingSettings
from services.recommendation import RecommendationService
from services.versions import code_version
from storage.requests import RequestLog


LOGGER = logging.getLogger(__name__)


class LoggedFastAPI(FastAPI):
    def build_middleware_stack(self):
        return RequestLoggingMiddleware(super().build_middleware_stack(), self.state)


def create_app(settings: ServingSettings | None = None) -> FastAPI:
    settings = settings or ServingSettings.from_environment()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.request_log = None
        app.state.request_log_error = None
        app.state.request_log_capture_failed = 0
        app.state.request_log_unavailable = 0
        app.state.code_version = await asyncio.to_thread(code_version, Path(__file__).resolve().parents[1])
        try:
            app.state.request_log = await asyncio.to_thread(
                RequestLog, settings.storage_path, settings.request_log_queue_size,
                settings.storage_busy_timeout,
            )
        except Exception as error:
            app.state.request_log_error = type(error).__name__
            LOGGER.error("Request logging unavailable at startup (%s)", type(error).__name__)
        try:
            app.state.service = await asyncio.to_thread(RecommendationService.load, settings)
            app.state.profile_import_error = getattr(app.state.service, "profile_import_error", None)
        except Exception as error:
            LOGGER.error("Serving resources could not be loaded (%s); readiness will return 503", type(error).__name__)
        try:
            yield
        finally:
            app.state.service = None
            if app.state.request_log is not None:
                drained = await asyncio.to_thread(
                    app.state.request_log.close, settings.request_log_shutdown_timeout,
                )
                if not drained:
                    LOGGER.error("Request logging shutdown left unpersisted records")

    app = LoggedFastAPI(title="Movie Recommendation API", lifespan=lifespan)
    app.state.service = None
    app.state.request_log = None
    app.state.request_log_error = None
    app.state.request_log_capture_failed = 0
    app.state.request_log_unavailable = 0
    app.state.code_version = None
    app.state.profile_import_error = None
    app.state.settings = settings
    app.include_router(router)
    return app


app = create_app()
