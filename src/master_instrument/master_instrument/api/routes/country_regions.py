"""CountryRegion API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from master_instrument.schemas.country_region import CountryRegionResponse
from master_instrument.services.country_region import CountryRegionService
from master_instrument.dependencies import get_country_region_service

router = APIRouter()


@router.get("", response_model=list[CountryRegionResponse])
def list_country_regions(
    service: Annotated[CountryRegionService, Depends(get_country_region_service)],
):
    """List all country-region associations."""
    return service.get_all()


@router.get("/region/{region_id}", response_model=list[CountryRegionResponse])
def get_countries_by_region(
    region_id: int,
    service: Annotated[CountryRegionService, Depends(get_country_region_service)],
):
    """Get all countries in a region."""
    return service.get_by_region(region_id)


@router.get("/country/{country_id}", response_model=list[CountryRegionResponse])
def get_regions_by_country(
    country_id: int,
    service: Annotated[CountryRegionService, Depends(get_country_region_service)],
):
    """Get all regions for a country."""
    return service.get_by_country(country_id)
