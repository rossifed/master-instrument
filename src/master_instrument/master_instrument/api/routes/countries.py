"""Country API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.country import CountryResponse
from master_instrument.dependencies import get_country_service
from master_instrument.services.country import CountryService

router = APIRouter()


@router.get("", response_model=list[CountryResponse])
def list_countries(
    service: Annotated[CountryService, Depends(get_country_service)],
):
    """List all countries."""
    return service.get_all()


@router.get("/{country_id}", response_model=CountryResponse)
def get_country(
    country_id: int,
    service: Annotated[CountryService, Depends(get_country_service)],
):
    """Get a single country by ID."""
    country = service.get(country_id)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country


@router.get("/alpha2/{code}", response_model=CountryResponse)
def get_country_by_alpha2(
    code: str,
    service: Annotated[CountryService, Depends(get_country_service)],
):
    """Get a country by alpha2 code (e.g. 'US', 'FR')."""
    country = service.get_by_alpha2(code)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country


@router.get("/alpha3/{code}", response_model=CountryResponse)
def get_country_by_alpha3(
    code: str,
    service: Annotated[CountryService, Depends(get_country_service)],
):
    """Get a country by alpha3 code (e.g. 'USA', 'FRA')."""
    country = service.get_by_alpha3(code)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country
