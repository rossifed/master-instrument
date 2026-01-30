"""EntityClassification API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from master_instrument.schemas.classification import EntityClassificationResponse
from master_instrument.services.classification import EntityClassificationService
from master_instrument.dependencies import get_entity_classification_service

router = APIRouter()


@router.get("", response_model=list[EntityClassificationResponse])
def list_entity_classifications(
    service: Annotated[EntityClassificationService, Depends(get_entity_classification_service)],
    entity_id: int | None = Query(None, description="Filter by entity"),
):
    """List entity classifications for an entity."""
    if entity_id:
        return service.get_by_entity(entity_id)
    return service.get_all()


@router.get("/scheme/{scheme_id}", response_model=list[EntityClassificationResponse])
def get_classifications_by_scheme(
    scheme_id: int,
    service: Annotated[EntityClassificationService, Depends(get_entity_classification_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get all company classifications for a scheme (e.g. GICS)."""
    return service.get_by_scheme(scheme_id, skip=skip, limit=limit)
