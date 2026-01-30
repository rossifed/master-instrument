"""VenueType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.venue_type import VenueTypeResponse
from master_instrument.services.venue_type import VenueTypeService
from master_instrument.dependencies import get_venue_type_service

router = APIRouter()


@router.get("", response_model=list[VenueTypeResponse])
def list_venue_types(
    service: Annotated[VenueTypeService, Depends(get_venue_type_service)],
):
    """List all venue types."""
    return service.get_all()


@router.get("/{venue_type_id}", response_model=VenueTypeResponse)
def get_venue_type(
    venue_type_id: int,
    service: Annotated[VenueTypeService, Depends(get_venue_type_service)],
):
    """Get a venue type by ID."""
    item = service.get(venue_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="VenueType not found")
    return item


@router.get("/mnemonic/{mnemonic}", response_model=VenueTypeResponse)
def get_venue_type_by_mnemonic(
    mnemonic: str,
    service: Annotated[VenueTypeService, Depends(get_venue_type_service)],
):
    """Get a venue type by mnemonic."""
    item = service.get_by_mnemonic(mnemonic)
    if not item:
        raise HTTPException(status_code=404, detail="VenueType not found")
    return item
