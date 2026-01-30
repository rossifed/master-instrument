"""Instrument service - simple CRUD."""

from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session

from master_instrument.db.models import Instrument


class InstrumentService:
    """Service for Instrument operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, instrument_id: int) -> Instrument | None:
        """Get a single instrument by ID."""
        return self.db.get(Instrument, instrument_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Instrument]:
        """Get all instruments with pagination."""
        stmt = select(Instrument).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def search(self, q: str, skip: int = 0, limit: int = 100) -> list[Instrument]:
        """Search instruments by name OR symbol (ILIKE)."""
        pattern = f"%{q}%"
        stmt = (
            select(Instrument)
            .where(or_(
                Instrument.name.ilike(pattern),
                Instrument.symbol.ilike(pattern)
            ))
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_symbol(self, symbol: str) -> Instrument | None:
        """Get instrument by exact symbol."""
        stmt = select(Instrument).where(Instrument.symbol == symbol)
        return self.db.scalar(stmt)

    def get_by_entity(self, entity_id: int, skip: int = 0, limit: int = 100) -> list[Instrument]:
        """Get all instruments for an entity."""
        stmt = (
            select(Instrument)
            .where(Instrument.entity_id == entity_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def count(self) -> int:
        """Count total instruments."""
        stmt = select(func.count()).select_from(Instrument)
        return self.db.scalar(stmt) or 0
