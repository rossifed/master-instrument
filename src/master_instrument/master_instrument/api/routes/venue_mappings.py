"""VenueMapping API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.mapping import VenueMappingResponse
from master_instrument.services.mapping import VenueMappingService
from master_instrument.dependencies import get_venue_mapping_service

router = APIRouter()


@router.get("", response_model=list[VenueMappingResponse])
def list_venue_mappings(
    service: Annotated[VenueMappingService, Depends(get_venue_mapping_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all venue mappings."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/{internal_venue_id}", response_model=VenueMappingResponse)
def get_venue_mapping(
    internal_venue_id: int,
    service: Annotated[VenueMappingService, Depends(get_venue_mapping_service)],
):
    """Get a venue mapping by internal ID."""
    mapping = service.get(internal_venue_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="VenueMapping not found")
    return mapping


@router.get("/external/{external_id}", response_model=list[VenueMappingResponse])
def get_venue_mapping_by_external_id(
    external_id: str,
    service: Annotated[VenueMappingService, Depends(get_venue_mapping_service)],
    data_source_id: int | None = Query(None),
):
    """Get venue mappings by external ID."""
    return service.get_by_external_id(external_id, data_source_id)
