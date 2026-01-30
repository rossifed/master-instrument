"""Venue API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.venue import VenueResponse
from master_instrument.dependencies import get_venue_service
from master_instrument.services.venue import VenueService

router = APIRouter()


@router.get("", response_model=list[VenueResponse])
def list_venues(
    service: Annotated[VenueService, Depends(get_venue_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all venues with their venue_type."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/{venue_id}", response_model=VenueResponse)
def get_venue(
    venue_id: int,
    service: Annotated[VenueService, Depends(get_venue_service)],
):
    """Get a single venue by ID."""
    venue = service.get(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    return venue


@router.get("/mnemonic/{mnemonic}", response_model=VenueResponse)
def get_venue_by_mnemonic(
    mnemonic: str,
    service: Annotated[VenueService, Depends(get_venue_service)],
):
    """Get a venue by mnemonic."""
    venue = service.get_by_mnemonic(mnemonic)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    return venue
