"""AdjustedMarketData API routes - corporate action adjusted prices."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from master_instrument.dependencies import get_adjusted_market_data_service
from master_instrument.schemas.adjusted_market_data import AdjustedMarketDataResponse
from master_instrument.services.adjusted_market_data import AdjustedMarketDataService

router = APIRouter()


# ===== By quote_id =====

@router.get("/quote/{quote_id}", response_model=list[AdjustedMarketDataResponse])
def get_adjusted_market_data_by_quote(
    quote_id: int,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[AdjustedMarketDataResponse]:
    """Get corporate action adjusted market data for a quote (most recent first)."""
    return service.get_by_quote_id(quote_id, skip=skip, limit=limit)


@router.get("/quote/{quote_id}/date/{trade_date}", response_model=AdjustedMarketDataResponse)
def get_adjusted_market_data_by_quote_and_date(
    quote_id: int,
    trade_date: date,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
) -> AdjustedMarketDataResponse:
    """Get adjusted market data for a specific quote and date."""
    result = service.get_by_quote_and_date(quote_id, trade_date)
    if not result:
        raise HTTPException(status_code=404, detail="Adjusted market data not found")
    return result


@router.get("/quote/{quote_id}/range", response_model=list[AdjustedMarketDataResponse])
def get_adjusted_market_data_by_quote_date_range(
    quote_id: int,
    start_date: date,
    end_date: date,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
) -> list[AdjustedMarketDataResponse]:
    """Get adjusted market data for a quote within a date range."""
    return service.get_by_quote_date_range(
        quote_id, start_date, end_date, skip=skip, limit=limit
    )


@router.get("/quote/{quote_id}/latest", response_model=AdjustedMarketDataResponse)
def get_latest_adjusted_market_data_by_quote(
    quote_id: int,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
) -> AdjustedMarketDataResponse:
    """Get the most recent adjusted market data for a quote."""
    result = service.get_latest_by_quote(quote_id)
    if not result:
        raise HTTPException(status_code=404, detail="Adjusted market data not found")
    return result


# ===== By instrument_id (via primary quote) =====

@router.get("/instrument/{instrument_id}", response_model=list[AdjustedMarketDataResponse])
def get_adjusted_market_data_by_instrument(
    instrument_id: int,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> list[AdjustedMarketDataResponse]:
    """Get adjusted market data for an instrument's primary quote (most recent first)."""
    quote_id, data = service.get_by_instrument_id(instrument_id, skip=skip, limit=limit)
    if quote_id is None:
        raise HTTPException(status_code=404, detail="No primary quote found for instrument")
    return data


@router.get("/instrument/{instrument_id}/range", response_model=list[AdjustedMarketDataResponse])
def get_adjusted_market_data_by_instrument_date_range(
    instrument_id: int,
    start_date: date,
    end_date: date,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
) -> list[AdjustedMarketDataResponse]:
    """Get adjusted market data for an instrument's primary quote within a date range."""
    quote_id, data = service.get_by_instrument_date_range(
        instrument_id, start_date, end_date, skip=skip, limit=limit
    )
    if quote_id is None:
        raise HTTPException(status_code=404, detail="No primary quote found for instrument")
    return data


@router.get("/instrument/{instrument_id}/latest", response_model=AdjustedMarketDataResponse)
def get_latest_adjusted_market_data_by_instrument(
    instrument_id: int,
    service: Annotated[AdjustedMarketDataService, Depends(get_adjusted_market_data_service)],
) -> AdjustedMarketDataResponse:
    """Get the most recent adjusted market data for an instrument's primary quote."""
    quote_id, result = service.get_latest_by_instrument(instrument_id)
    if quote_id is None:
        raise HTTPException(status_code=404, detail="No primary quote found for instrument")
    if not result:
        raise HTTPException(status_code=404, detail="Adjusted market data not found")
    return result
