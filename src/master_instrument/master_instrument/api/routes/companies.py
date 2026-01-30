"""Company API routes - simple CRUD."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.company import CompanyResponse
from master_instrument.dependencies import get_company_service
from master_instrument.services.company import CompanyService

router = APIRouter()


@router.get("", response_model=list[CompanyResponse])
def list_companies(
    service: Annotated[CompanyService, Depends(get_company_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all companies."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/search", response_model=list[CompanyResponse])
def search_companies(
    service: Annotated[CompanyService, Depends(get_company_service)],
    name: str = Query(..., min_length=1, description="Search by company name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Search companies by name."""
    return service.search_by_name(name, skip=skip, limit=limit)


@router.get("/{company_id}", response_model=CompanyResponse)
def get_company(
    company_id: int,
    service: Annotated[CompanyService, Depends(get_company_service)],
):
    """Get a single company by ID."""
    company = service.get(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company
