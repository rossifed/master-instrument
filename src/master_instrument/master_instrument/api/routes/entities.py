"""Entity API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.entity import EntityResponse
from master_instrument.dependencies import get_entity_service
from master_instrument.services.entity import EntityService

router = APIRouter()


@router.get("", response_model=list[EntityResponse])
def list_entities(
    service: Annotated[EntityService, Depends(get_entity_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all entities."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/search", response_model=list[EntityResponse])
def search_entities(
    service: Annotated[EntityService, Depends(get_entity_service)],
    name: str = Query(..., min_length=1, description="Search by name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Search entities by name."""
    return service.search(name, skip=skip, limit=limit)


@router.get("/type/{entity_type_id}", response_model=list[EntityResponse])
def get_entities_by_type(
    entity_type_id: int,
    service: Annotated[EntityService, Depends(get_entity_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get entities by type."""
    return service.get_by_type(entity_type_id, skip=skip, limit=limit)


@router.get("/{entity_id}", response_model=EntityResponse)
def get_entity(
    entity_id: int,
    service: Annotated[EntityService, Depends(get_entity_service)],
):
    """Get a single entity by ID."""
    entity = service.get(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity
