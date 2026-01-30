"""InstrumentType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.instrument_type import InstrumentTypeResponse
from master_instrument.services.instrument_type import InstrumentTypeService
from master_instrument.dependencies import get_instrument_type_service

router = APIRouter()


@router.get("", response_model=list[InstrumentTypeResponse])
def list_instrument_types(
    service: Annotated[InstrumentTypeService, Depends(get_instrument_type_service)],
):
    """List all instrument types."""
    return service.get_all()


@router.get("/{instrument_type_id}", response_model=InstrumentTypeResponse)
def get_instrument_type(
    instrument_type_id: int,
    service: Annotated[InstrumentTypeService, Depends(get_instrument_type_service)],
):
    """Get an instrument type by ID."""
    item = service.get(instrument_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="InstrumentType not found")
    return item


@router.get("/mnemonic/{mnemonic}", response_model=InstrumentTypeResponse)
def get_instrument_type_by_mnemonic(
    mnemonic: str,
    service: Annotated[InstrumentTypeService, Depends(get_instrument_type_service)],
):
    """Get an instrument type by mnemonic."""
    item = service.get_by_mnemonic(mnemonic)
    if not item:
        raise HTTPException(status_code=404, detail="InstrumentType not found")
    return item
