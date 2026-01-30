"""CompanyMarketCap API routes."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from master_instrument.dependencies import get_company_market_cap_service
from master_instrument.schemas.company_market_cap import CompanyMarketCapResponse
from master_instrument.services.company_market_cap import CompanyMarketCapService

router = APIRouter(tags=["company-market-caps"])


@router.get("/company/{company_id}", response_model=list[CompanyMarketCapResponse])
def get_market_caps_by_company(
    company_id: int,
    service: Annotated[CompanyMarketCapService, Depends(get_company_market_cap_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[CompanyMarketCapResponse]:
    """Get market cap history for a company (most recent first)."""
    return service.get_by_company_id(company_id, skip=skip, limit=limit)


@router.get("/company/{company_id}/date/{valuation_date}", response_model=CompanyMarketCapResponse)
def get_market_cap_by_company_and_date(
    company_id: int,
    valuation_date: date,
    service: Annotated[CompanyMarketCapService, Depends(get_company_market_cap_service)],
) -> CompanyMarketCapResponse:
    """Get market cap for a specific company and date."""
    result = service.get_by_company_and_date(company_id, valuation_date)
    if not result:
        raise HTTPException(status_code=404, detail="Market cap not found")
    return result


@router.get("/company/{company_id}/range", response_model=list[CompanyMarketCapResponse])
def get_market_caps_by_company_date_range(
    company_id: int,
    start_date: date,
    end_date: date,
    service: Annotated[CompanyMarketCapService, Depends(get_company_market_cap_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
) -> list[CompanyMarketCapResponse]:
    """Get market cap for a company within a date range."""
    return service.get_by_company_date_range(
        company_id, start_date, end_date, skip=skip, limit=limit
    )


@router.get("/date/{valuation_date}", response_model=list[CompanyMarketCapResponse])
def get_market_caps_by_date(
    valuation_date: date,
    service: Annotated[CompanyMarketCapService, Depends(get_company_market_cap_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[CompanyMarketCapResponse]:
    """Get all market caps for a specific date."""
    return service.get_by_date(valuation_date, skip=skip, limit=limit)


@router.get("/company/{company_id}/latest", response_model=CompanyMarketCapResponse)
def get_latest_market_cap_by_company(
    company_id: int,
    service: Annotated[CompanyMarketCapService, Depends(get_company_market_cap_service)],
) -> CompanyMarketCapResponse:
    """Get the most recent market cap for a company."""
    result = service.get_latest_by_company(company_id)
    if not result:
        raise HTTPException(status_code=404, detail="Market cap not found")
    return result
