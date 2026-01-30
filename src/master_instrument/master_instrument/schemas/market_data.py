"""MarketData Pydantic schemas."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class CurrencyRef(BaseModel):
    """Currency reference."""
    currency_id: int
    iso_code: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class QuoteRef(BaseModel):
    """Quote reference for market data."""
    quote_id: int
    ric: str | None = None
    ticker: str | None = None
    mic: str | None = None
    model_config = ConfigDict(from_attributes=True)


class MarketDataResponse(BaseModel):
    """MarketData response schema."""
    trade_date: date
    quote_id: int
    currency_id: int
    
    # OHLCV
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int | None = None
    
    # Bid/Ask
    bid: float | None = None
    ask: float | None = None
    
    # VWAP
    vwap: float | None = None
    
    # Audit
    loaded_at: datetime
    
    # Relations
    quote: QuoteRef | None = None
    currency: CurrencyRef | None = None
    
    model_config = ConfigDict(from_attributes=True)
