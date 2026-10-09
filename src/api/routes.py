"""HTTP routes for the milestone recommendation endpoint."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import ValidationError

from services.recommendation import RecommendationService
from api.request_logging import storage_status


router = APIRouter()
LOGGER = logging.getLogger(__name__)


def get_service(request: Request) -> RecommendationService:
    service = request.app.state.service
    if service is None:
        request.state.error_type = "ServiceUnavailable"
        raise HTTPException(503, "Recommendation service is not ready")
    request.state.serving_versions = service.versions
    return service


Service = Annotated[RecommendationService, Depends(get_service)]


@router.get("/health/ready")
def readiness(service: Service) -> dict[str, str]:
    return {"status": "ready"}


@router.get("/health/storage")
def storage_health(request: Request) -> JSONResponse:
    status = storage_status(request.app.state)
    return JSONResponse(status, status_code=200 if status["healthy"] else 503)


@router.get("/recommend/{userid}", response_class=PlainTextResponse)
def recommend(
    request: Request,
    userid: Annotated[int, Path(gt=0, le=2**63 - 1)],
    service: Service,
) -> PlainTextResponse:
    request.state.user_id = userid
    try:
        result = service.recommend(userid)
    except (ValidationError, ValueError, OSError) as error:
        request.state.error_type = type(error).__name__
        LOGGER.error("Could not serve request %s (%s)", request.state.request_id, type(error).__name__)
        raise HTTPException(503, "Recommendations are unavailable") from None
    request.state.recommendation_result = result
    return PlainTextResponse(
        ",".join(item.movie_id for item in result.recommendations)
    )
