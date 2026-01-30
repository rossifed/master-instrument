"""DividendType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.dividend_type import DividendTypeResponse
from master_instrument.services.dividend_type import DividendTypeService
from master_instrument.dependencies import get_dividend_type_service

router = APIRouter()


@router.get("", response_model=list[DividendTypeResponse])
def list_dividend_types(
    service: Annotated[DividendTypeService, Depends(get_dividend_type_service)],
):
    """List all dividend types."""
    return service.get_all()


@router.get("/{dividend_type_id}", response_model=DividendTypeResponse)
def get_dividend_type(
    dividend_type_id: int,
    service: Annotated[DividendTypeService, Depends(get_dividend_type_service)],
):
    """Get a dividend type by ID."""
    item = service.get(dividend_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="DividendType not found")
    return item


@router.get("/code/{code}", response_model=DividendTypeResponse)
def get_dividend_type_by_code(
    code: str,
    service: Annotated[DividendTypeService, Depends(get_dividend_type_service)],
):
    """Get a dividend type by code."""
    item = service.get_by_code(code)
    if not item:
        raise HTTPException(status_code=404, detail="DividendType not found")
    return item
