"""EquityType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.equity_type import EquityTypeResponse
from master_instrument.services.equity_type import EquityTypeService
from master_instrument.dependencies import get_equity_type_service

router = APIRouter()


@router.get("", response_model=list[EquityTypeResponse])
def list_equity_types(
    service: Annotated[EquityTypeService, Depends(get_equity_type_service)],
):
    """List all equity types."""
    return service.get_all()


@router.get("/{equity_type_id}", response_model=EquityTypeResponse)
def get_equity_type(
    equity_type_id: int,
    service: Annotated[EquityTypeService, Depends(get_equity_type_service)],
):
    """Get an equity type by ID."""
    item = service.get(equity_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="EquityType not found")
    return item


@router.get("/mnemonic/{mnemonic}", response_model=EquityTypeResponse)
def get_equity_type_by_mnemonic(
    mnemonic: str,
    service: Annotated[EquityTypeService, Depends(get_equity_type_service)],
):
    """Get an equity type by mnemonic."""
    item = service.get_by_mnemonic(mnemonic)
    if not item:
        raise HTTPException(status_code=404, detail="EquityType not found")
    return item
