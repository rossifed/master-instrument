"""View model for corporate action adjusted market data.

This maps to the dbt view vw_market_data_corp_adjusted.
Inherits from ViewBase to avoid Alembic management.
"""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, Double, Integer, String, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from .base import ViewBase


class VwMarketDataCorpAdjusted(ViewBase):
    """Corporate action adjusted market data view.
    
    Read-only view created by dbt. Provides market data with
    corporate action adjustment factors applied.
    """
    __tablename__ = "vw_market_data_corp_adjusted"
    __table_args__ = {"schema": "master"}

    # Keys (composite for uniqueness in ORM)
    quote_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    
    # Instrument info (denormalized from view)
    instrument_id: Mapped[int] = mapped_column(Integer)
    instrument_name: Mapped[str] = mapped_column(String)
    entity_id: Mapped[int] = mapped_column(Integer)
    entity_name: Mapped[str] = mapped_column(String)
    currency_id: Mapped[int] = mapped_column(Integer)
    currency_code: Mapped[str] = mapped_column(String)
    is_primary: Mapped[bool] = mapped_column(Boolean)
    price_unit: Mapped[float] = mapped_column(Double)
    
    # Adjustment factor
    cum_adj_factor: Mapped[float] = mapped_column(Double)
    
    # Raw prices
    open: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    close: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    bid: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    vwap: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Adjusted prices
    adjusted_open: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_high: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_low: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_close: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_bid: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_ask: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_vwap: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    adjusted_volume: Mapped[Optional[float]] = mapped_column(Double, nullable=True)

    def __repr__(self) -> str:
        return f"<VwMarketDataCorpAdjusted quote={self.quote_id} date={self.trade_date} adj={self.cum_adj_factor}>"
