"""HTTP routes for the milestone recommendation endpoint."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi.responses import PlainTextResponse
from pydantic import ValidationError

from services.recommendation import RecommendationService


router = APIRouter()
LOGGER = logging.getLogger(__name__)


def get_service(request: Request) -> RecommendationService:
    service = request.app.state.service
    if service is None:
        raise HTTPException(503, "Recommendation service is not ready")
    return service


Service = Annotated[RecommendationService, Depends(get_service)]


@router.get("/health/ready")
def readiness(service: Service) -> dict[str, str]:
    return {"status": "ready"}


@router.get("/recommend/{userid}", response_class=PlainTextResponse)
def recommend(
    userid: Annotated[int, Path(gt=0, le=2**63 - 1)],
    service: Service,
) -> PlainTextResponse:
    try:
        result = service.recommend(userid)
    except (ValidationError, ValueError, OSError):
        LOGGER.exception("Could not serve recommendations for userid=%s", userid)
        raise HTTPException(503, "Recommendations are unavailable") from None
    return PlainTextResponse(
        ",".join(item.movie_id for item in result.recommendations)
    )
