"""Create the FastAPI application and load serving resources once at startup."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import router
from models.settings import ServingSettings
from services.recommendation import RecommendationService


LOGGER = logging.getLogger(__name__)


def create_app(settings: ServingSettings | None = None) -> FastAPI:
    settings = settings or ServingSettings.from_environment()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            app.state.service = await asyncio.to_thread(RecommendationService.load, settings)
        except Exception:
            LOGGER.exception("Serving resources could not be loaded; readiness will return 503")
        try:
            yield
        finally:
            app.state.service = None

    app = FastAPI(title="Movie Recommendation API", lifespan=lifespan)
    app.state.service = None
    app.include_router(router)
    return app


app = create_app()
