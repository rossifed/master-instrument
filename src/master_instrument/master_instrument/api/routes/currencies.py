"""Currency API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.currency import CurrencyResponse
from master_instrument.dependencies import get_currency_service
from master_instrument.services.currency import CurrencyService

router = APIRouter()


@router.get("", response_model=list[CurrencyResponse])
def list_currencies(
    service: Annotated[CurrencyService, Depends(get_currency_service)],
):
    """List all currencies."""
    return service.get_all()


@router.get("/{currency_id}", response_model=CurrencyResponse)
def get_currency(
    currency_id: int,
    service: Annotated[CurrencyService, Depends(get_currency_service)],
):
    """Get a single currency by ID."""
    currency = service.get(currency_id)
    if not currency:
        raise HTTPException(status_code=404, detail="Currency not found")
    return currency


@router.get("/code/{code}", response_model=CurrencyResponse)
def get_currency_by_code(
    code: str,
    service: Annotated[CurrencyService, Depends(get_currency_service)],
):
    """Get a currency by code (e.g. 'USD', 'EUR')."""
    currency = service.get_by_code(code)
    if not currency:
        raise HTTPException(status_code=404, detail="Currency not found")
    return currency
