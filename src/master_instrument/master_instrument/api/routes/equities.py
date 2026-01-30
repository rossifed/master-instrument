"""Equity API routes - simple CRUD."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.equity import EquityResponse
from master_instrument.dependencies import get_equity_service
from master_instrument.services.equity import EquityService

router = APIRouter()


@router.get("", response_model=list[EquityResponse])
def list_equities(
    service: Annotated[EquityService, Depends(get_equity_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all equities."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/search", response_model=list[EquityResponse])
def search_equities(
    service: Annotated[EquityService, Depends(get_equity_service)],
    name: str = Query(..., min_length=1, description="Search by company name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Search equities by company name."""
    return service.search(name, skip=skip, limit=limit)


@router.get("/major", response_model=list[EquityResponse])
def list_major_equities(
    service: Annotated[EquityService, Depends(get_equity_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List only major securities."""
    return service.get_major_securities(skip=skip, limit=limit)


@router.get("/{equity_id}", response_model=EquityResponse)
def get_equity(
    equity_id: int,
    service: Annotated[EquityService, Depends(get_equity_service)],
):
    """Get a single equity by ID."""
    equity = service.get(equity_id)
    if not equity:
        raise HTTPException(status_code=404, detail="Equity not found")
    return equity


@router.get("/isin/{isin}", response_model=EquityResponse)
def get_equity_by_isin(
    isin: str,
    service: Annotated[EquityService, Depends(get_equity_service)],
):
    """Get equity by ISIN."""
    equity = service.get_by_isin(isin)
    if not equity:
        raise HTTPException(status_code=404, detail="Equity not found")
    return equity


@router.get("/ric/{ric}", response_model=EquityResponse)
def get_equity_by_ric(
    ric: str,
    service: Annotated[EquityService, Depends(get_equity_service)],
):
    """Get equity by RIC."""
    equity = service.get_by_ric(ric)
    if not equity:
        raise HTTPException(status_code=404, detail="Equity not found")
    return equity


@router.get("/sedol/{sedol}", response_model=EquityResponse)
def get_equity_by_sedol(
    sedol: str,
    service: Annotated[EquityService, Depends(get_equity_service)],
):
    """Get equity by SEDOL."""
    equity = service.get_by_sedol(sedol)
    if not equity:
        raise HTTPException(status_code=404, detail="Equity not found")
    return equity


@router.get("/ticker/{ticker}", response_model=list[EquityResponse])
def get_equities_by_ticker(
    ticker: str,
    service: Annotated[EquityService, Depends(get_equity_service)],
):
    """Get equities by ticker (can return multiple)."""
    return service.get_by_ticker(ticker)
