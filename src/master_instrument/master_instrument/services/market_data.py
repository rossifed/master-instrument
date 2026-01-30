"""MarketData service - timeseries data access."""

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import MarketData
from master_instrument.db.models import Quote


class MarketDataService:
    """Service for MarketData operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_quote_id(
        self, quote_id: int, skip: int = 0, limit: int = 100
    ) -> list[MarketData]:
        """Get market data for a quote (most recent first)."""
        stmt = (
            select(MarketData)
            .where(MarketData.quote_id == quote_id)
            .order_by(MarketData.trade_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_quote_and_date(self, quote_id: int, trade_date: date) -> MarketData | None:
        """Get market data for a specific quote and date (PK lookup)."""
        return self.db.get(MarketData, (trade_date, quote_id))

    def get_by_quote_date_range(
        self,
        quote_id: int,
        start_date: date,
        end_date: date,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[MarketData]:
        """Get market data for a quote within a date range."""
        stmt = (
            select(MarketData)
            .where(
                MarketData.quote_id == quote_id,
                MarketData.trade_date >= start_date,
                MarketData.trade_date <= end_date,
            )
            .order_by(MarketData.trade_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_date(
        self, trade_date: date, skip: int = 0, limit: int = 100
    ) -> list[MarketData]:
        """Get all market data for a specific date."""
        stmt = (
            select(MarketData)
            .where(MarketData.trade_date == trade_date)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_latest_by_quote(self, quote_id: int) -> MarketData | None:
        """Get the most recent market data for a quote."""
        stmt = (
            select(MarketData)
            .where(MarketData.quote_id == quote_id)
            .order_by(MarketData.trade_date.desc())
            .limit(1)
        )
        return self.db.scalar(stmt)

    def _get_primary_quote_id(self, instrument_id: int) -> int | None:
        """Get the primary quote_id for an instrument."""
        stmt = (
            select(Quote.quote_id)
            .where(Quote.instrument_id == instrument_id, Quote.is_primary == True)
            .limit(1)
        )
        return self.db.scalar(stmt)

    def get_by_instrument_id(
        self, instrument_id: int, skip: int = 0, limit: int = 100
    ) -> tuple[int | None, list[MarketData]]:
        """Get market data for an instrument's primary quote.
        
        Returns tuple of (quote_id, market_data_list).
        """
        quote_id = self._get_primary_quote_id(instrument_id)
        if quote_id is None:
            return None, []
        return quote_id, self.get_by_quote_id(quote_id, skip=skip, limit=limit)

    def get_by_instrument_date_range(
        self,
        instrument_id: int,
        start_date: date,
        end_date: date,
        skip: int = 0,
        limit: int = 1000,
    ) -> tuple[int | None, list[MarketData]]:
        """Get market data for an instrument's primary quote within a date range."""
        quote_id = self._get_primary_quote_id(instrument_id)
        if quote_id is None:
            return None, []
        return quote_id, self.get_by_quote_date_range(
            quote_id, start_date, end_date, skip=skip, limit=limit
        )

    def get_latest_by_instrument(self, instrument_id: int) -> tuple[int | None, MarketData | None]:
        """Get the most recent market data for an instrument's primary quote."""
        quote_id = self._get_primary_quote_id(instrument_id)
        if quote_id is None:
            return None, None
        return quote_id, self.get_latest_by_quote(quote_id)
