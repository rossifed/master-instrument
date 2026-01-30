"""Quote API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.quote import QuoteResponse
from master_instrument.dependencies import get_quote_service
from master_instrument.services.quote import QuoteService

router = APIRouter()


@router.get("", response_model=list[QuoteResponse])
def list_quotes(
    service: Annotated[QuoteService, Depends(get_quote_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all quotes."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/primary", response_model=list[QuoteResponse])
def list_primary_quotes(
    service: Annotated[QuoteService, Depends(get_quote_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List only primary quotes."""
    return service.get_primary_quotes(skip=skip, limit=limit)


@router.get("/instrument/{instrument_id}", response_model=list[QuoteResponse])
def get_quotes_by_instrument(
    instrument_id: int,
    service: Annotated[QuoteService, Depends(get_quote_service)],
):
    """Get all quotes for an instrument."""
    return service.get_by_instrument(instrument_id)


@router.get("/ric/{ric}", response_model=QuoteResponse)
def get_quote_by_ric(
    ric: str,
    service: Annotated[QuoteService, Depends(get_quote_service)],
):
    """Get quote by RIC."""
    quote = service.get_by_ric(ric)
    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")
    return quote


@router.get("/ticker/{ticker}", response_model=list[QuoteResponse])
def get_quotes_by_ticker(
    ticker: str,
    service: Annotated[QuoteService, Depends(get_quote_service)],
):
    """Get quotes by ticker."""
    return service.get_by_ticker(ticker)


@router.get("/mic/{mic}", response_model=list[QuoteResponse])
def get_quotes_by_mic(
    mic: str,
    service: Annotated[QuoteService, Depends(get_quote_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get quotes by MIC (market)."""
    return service.get_by_mic(mic, skip=skip, limit=limit)


@router.get("/{quote_id}", response_model=QuoteResponse)
def get_quote(
    quote_id: int,
    service: Annotated[QuoteService, Depends(get_quote_service)],
):
    """Get a single quote by ID."""
    quote = service.get(quote_id)
    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")
    return quote
