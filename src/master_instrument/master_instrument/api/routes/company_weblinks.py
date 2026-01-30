"""CompanyWeblink API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.company_weblink import CompanyWeblinkResponse
from master_instrument.services.company_weblink import CompanyWeblinkService
from master_instrument.dependencies import get_company_weblink_service

router = APIRouter()


@router.get("", response_model=list[CompanyWeblinkResponse])
def list_company_weblinks(
    service: Annotated[CompanyWeblinkService, Depends(get_company_weblink_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all company weblinks with pagination."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/company/{company_id}", response_model=list[CompanyWeblinkResponse])
def get_weblinks_by_company(
    company_id: int,
    service: Annotated[CompanyWeblinkService, Depends(get_company_weblink_service)],
):
    """Get all weblinks for a specific company."""
    return service.get_by_company(company_id)


@router.get("/type/{weblink_type_id}", response_model=list[CompanyWeblinkResponse])
def get_weblinks_by_type(
    weblink_type_id: int,
    service: Annotated[CompanyWeblinkService, Depends(get_company_weblink_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get all weblinks of a specific type."""
    return service.get_by_type(weblink_type_id, skip=skip, limit=limit)


@router.get("/{weblink_id}", response_model=CompanyWeblinkResponse)
def get_company_weblink(
    weblink_id: int,
    service: Annotated[CompanyWeblinkService, Depends(get_company_weblink_service)],
):
    """Get a single company weblink by ID."""
    weblink = service.get(weblink_id)
    if not weblink:
        raise HTTPException(status_code=404, detail="Company weblink not found")
    return weblink
