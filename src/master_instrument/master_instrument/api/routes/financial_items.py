"""StdFinancialItem API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from master_instrument.dependencies import get_std_financial_item_service
from master_instrument.schemas.std_financial_item import StdFinancialItemResponse
from master_instrument.services.std_financial_item import StdFinancialItemService

router = APIRouter(tags=["financial-items"])


@router.get("", response_model=list[StdFinancialItemResponse])
def get_financial_items(
    service: Annotated[StdFinancialItemService, Depends(get_std_financial_item_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialItemResponse]:
    """Get all financial items."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/{item_id}", response_model=StdFinancialItemResponse)
def get_financial_item(
    item_id: int,
    service: Annotated[StdFinancialItemService, Depends(get_std_financial_item_service)],
) -> StdFinancialItemResponse:
    """Get a financial item by ID."""
    result = service.get(item_id)
    if not result:
        raise HTTPException(status_code=404, detail="Financial item not found")
    return result


@router.get("/search/by-name", response_model=list[StdFinancialItemResponse])
def search_financial_items_by_name(
    name: str,
    service: Annotated[StdFinancialItemService, Depends(get_std_financial_item_service)],
) -> list[StdFinancialItemResponse]:
    """Search financial items by name (partial match)."""
    return service.get_by_name(name)


@router.get("/statement-type/{statement_type_id}", response_model=list[StdFinancialItemResponse])
def get_financial_items_by_statement_type(
    statement_type_id: int,
    service: Annotated[StdFinancialItemService, Depends(get_std_financial_item_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[StdFinancialItemResponse]:
    """Get financial items by statement type."""
    return service.get_by_statement_type(statement_type_id, skip=skip, limit=limit)
