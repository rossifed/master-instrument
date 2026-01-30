"""Venue service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Venue


class VenueService:
    """Service for Venue operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, venue_id: int) -> Venue | None:
        """Get a single venue by ID."""
        return self.db.get(Venue, venue_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Venue]:
        """Get all venues with their venue_type (eager loaded via selectin)."""
        stmt = select(Venue).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> Venue | None:
        """Get venue by mnemonic."""
        stmt = select(Venue).where(Venue.mnemonic == mnemonic)
        return self.db.scalar(stmt)
