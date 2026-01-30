"""StdFinancialValue API routes."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from master_instrument.dependencies import get_std_financial_value_service
from master_instrument.schemas.std_financial_value import StdFinancialValueResponse
from master_instrument.services.std_financial_value import StdFinancialValueService

router = APIRouter(tags=["financial-values"])


@router.get("/", response_model=list[StdFinancialValueResponse])
def search_financial_values(
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    company_id: int | None = Query(None, description="Filter by company ID"),
    item_id: int | None = Query(None, description="Filter by financial item ID"),
    period_end_date: date | None = Query(None, description="Filter by period end date (YYYY-MM-DD)"),
    period_type_id: int | None = Query(None, description="Filter by period type ID (e.g., 1=Annual, 2=Quarterly)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Search financial values with flexible filters.
    
    All filters are optional. Combine them to narrow down results.
    """
    return service.search(
        company_id=company_id,
        item_id=item_id,
        period_end_date=period_end_date,
        period_type_id=period_type_id,
        skip=skip,
        limit=limit,
    )


@router.get("/company/{company_id}", response_model=list[StdFinancialValueResponse])
def get_financial_values_by_company(
    company_id: int,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Get all financial values for a company."""
    return service.get_by_company_id(company_id, skip=skip, limit=limit)


@router.get("/company/{company_id}/item/{item_id}", response_model=list[StdFinancialValueResponse])
def get_financial_values_by_company_and_item(
    company_id: int,
    item_id: int,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Get financial values for a company and specific item."""
    return service.get_by_company_and_item(company_id, item_id, skip=skip, limit=limit)


@router.get("/company/{company_id}/period/{period_end_date}", response_model=list[StdFinancialValueResponse])
def get_financial_values_by_company_and_period(
    company_id: int,
    period_end_date: date,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Get all financial values for a company at a specific period end date."""
    return service.get_by_company_and_period_date(
        company_id, period_end_date, skip=skip, limit=limit
    )


@router.get("/item/{item_id}/calendar-date/{calendar_end_date}", response_model=list[StdFinancialValueResponse])
def get_financial_values_by_item_and_calendar_date(
    item_id: int,
    calendar_end_date: date,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Get financial values for an item at a specific calendar date (cross-company)."""
    return service.get_by_item_and_calendar_date(
        item_id, calendar_end_date, skip=skip, limit=limit
    )


@router.get("/statement/{statement_id}", response_model=list[StdFinancialValueResponse])
def get_financial_values_by_statement(
    statement_id: int,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialValueResponse]:
    """Get all financial values for a statement."""
    return service.get_by_statement_id(statement_id, skip=skip, limit=limit)


@router.get("/statement/{statement_id}/item/{item_id}", response_model=StdFinancialValueResponse)
def get_financial_value_by_pk(
    statement_id: int,
    item_id: int,
    service: Annotated[StdFinancialValueService, Depends(get_std_financial_value_service)],
) -> StdFinancialValueResponse:
    """Get a specific financial value by composite primary key."""
    result = service.get_by_pk(statement_id, item_id)
    if not result:
        raise HTTPException(status_code=404, detail="Financial value not found")
    return result
