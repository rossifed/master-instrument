"""Quote service - simple CRUD."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Quote


class QuoteService:
    """Service for Quote operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, quote_id: int) -> Quote | None:
        """Get a single quote by ID."""
        return self.db.get(Quote, quote_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Quote]:
        """Get all quotes."""
        stmt = select(Quote).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_instrument(self, instrument_id: int) -> list[Quote]:
        """Get all quotes for an instrument."""
        stmt = select(Quote).where(Quote.instrument_id == instrument_id)
        return list(self.db.scalars(stmt).all())

    def get_by_ric(self, ric: str) -> Quote | None:
        """Get quote by RIC."""
        stmt = select(Quote).where(Quote.ric == ric)
        return self.db.scalar(stmt)

    def get_by_ticker(self, ticker: str) -> list[Quote]:
        """Get quotes by ticker."""
        stmt = select(Quote).where(Quote.ticker == ticker)
        return list(self.db.scalars(stmt).all())

    def get_by_mic(self, mic: str, skip: int = 0, limit: int = 100) -> list[Quote]:
        """Get quotes by MIC (market)."""
        stmt = select(Quote).where(Quote.mic == mic).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_primary_quotes(self, skip: int = 0, limit: int = 100) -> list[Quote]:
        """Get only primary quotes."""
        stmt = select(Quote).where(Quote.is_primary.is_(True)).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())
