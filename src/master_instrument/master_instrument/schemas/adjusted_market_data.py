"""AdjustedMarketData Pydantic schemas for corporate action adjusted prices."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class AdjustedMarketDataResponse(BaseModel):
    """Corporate action adjusted market data response."""
    
    # Keys
    quote_id: int
    trade_date: date
    
    # Instrument info
    instrument_id: int
    instrument_name: str
    entity_id: int
    entity_name: str
    currency_id: int
    currency_code: str
    is_primary: bool
    price_unit: float
    
    # Adjustment factor
    cum_adj_factor: float
    
    # Raw prices
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    bid: float | None = None
    ask: float | None = None
    vwap: float | None = None
    volume: int | None = None
    
    # Adjusted prices
    adjusted_open: float | None = None
    adjusted_high: float | None = None
    adjusted_low: float | None = None
    adjusted_close: float | None = None
    adjusted_bid: float | None = None
    adjusted_ask: float | None = None
    adjusted_vwap: float | None = None
    adjusted_volume: float | None = None

    model_config = ConfigDict(from_attributes=True)
