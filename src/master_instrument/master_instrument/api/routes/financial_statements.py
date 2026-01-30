"""StdFinancialStatement API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from master_instrument.dependencies import get_std_financial_statement_service
from master_instrument.schemas.std_financial_statement import StdFinancialStatementResponse
from master_instrument.services.std_financial_statement import StdFinancialStatementService

router = APIRouter(tags=["financial-statements"])


@router.get("", response_model=list[StdFinancialStatementResponse])
def get_financial_statements(
    service: Annotated[StdFinancialStatementService, Depends(get_std_financial_statement_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialStatementResponse]:
    """Get all financial statements."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/{statement_id}", response_model=StdFinancialStatementResponse)
def get_financial_statement(
    statement_id: int,
    service: Annotated[StdFinancialStatementService, Depends(get_std_financial_statement_service)],
) -> StdFinancialStatementResponse:
    """Get a financial statement by ID."""
    result = service.get(statement_id)
    if not result:
        raise HTTPException(status_code=404, detail="Financial statement not found")
    return result


@router.get("/filing/{filing_id}", response_model=list[StdFinancialStatementResponse])
def get_financial_statements_by_filing(
    filing_id: int,
    service: Annotated[StdFinancialStatementService, Depends(get_std_financial_statement_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialStatementResponse]:
    """Get statements for a filing."""
    return service.get_by_filing_id(filing_id, skip=skip, limit=limit)


@router.get("/statement-type/{statement_type_id}", response_model=list[StdFinancialStatementResponse])
def get_financial_statements_by_type(
    statement_type_id: int,
    service: Annotated[StdFinancialStatementService, Depends(get_std_financial_statement_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialStatementResponse]:
    """Get statements by type."""
    return service.get_by_statement_type(statement_type_id, skip=skip, limit=limit)
