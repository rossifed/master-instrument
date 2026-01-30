"""Instrument API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.instrument import InstrumentResponse
from master_instrument.dependencies import get_instrument_service
from master_instrument.services.instrument import InstrumentService

router = APIRouter()


@router.get("", response_model=list[InstrumentResponse])
def list_instruments(
    service: Annotated[InstrumentService, Depends(get_instrument_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all instruments with pagination."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/search", response_model=list[InstrumentResponse])
def search_instruments(
    service: Annotated[InstrumentService, Depends(get_instrument_service)],
    q: str = Query(..., min_length=1, description="Search by name or symbol (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Search instruments by name or symbol."""
    return service.search(q, skip=skip, limit=limit)


@router.get("/symbol/{symbol}", response_model=InstrumentResponse)
def get_instrument_by_symbol(
    symbol: str,
    service: Annotated[InstrumentService, Depends(get_instrument_service)],
):
    """Get instrument by exact symbol."""
    instrument = service.get_by_symbol(symbol)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")
    return instrument


@router.get("/entity/{entity_id}", response_model=list[InstrumentResponse])
def get_instruments_by_entity(
    entity_id: int,
    service: Annotated[InstrumentService, Depends(get_instrument_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get all instruments for an entity."""
    return service.get_by_entity(entity_id, skip=skip, limit=limit)


@router.get("/{instrument_id}", response_model=InstrumentResponse)
def get_instrument(
    instrument_id: int,
    service: Annotated[InstrumentService, Depends(get_instrument_service)],
):
    """Get a single instrument by ID."""
    instrument = service.get(instrument_id)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")
    return instrument
