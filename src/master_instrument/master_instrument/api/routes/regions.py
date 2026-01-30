"""Region API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.region import RegionResponse
from master_instrument.services.region import RegionService
from master_instrument.dependencies import get_region_service

router = APIRouter()


@router.get("", response_model=list[RegionResponse])
def list_regions(
    service: Annotated[RegionService, Depends(get_region_service)],
):
    """List all regions."""
    return service.get_all()


@router.get("/{region_id}", response_model=RegionResponse)
def get_region(
    region_id: int,
    service: Annotated[RegionService, Depends(get_region_service)],
):
    """Get a region by ID."""
    item = service.get(region_id)
    if not item:
        raise HTTPException(status_code=404, detail="Region not found")
    return item


@router.get("/code/{code}", response_model=RegionResponse)
def get_region_by_code(
    code: str,
    service: Annotated[RegionService, Depends(get_region_service)],
):
    """Get a region by code."""
    item = service.get_by_code(code)
    if not item:
        raise HTTPException(status_code=404, detail="Region not found")
    return item
