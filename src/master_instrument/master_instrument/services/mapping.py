"""Mapping services - simplified to venue and quote mappings only."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import VenueMapping, QuoteMapping


class VenueMappingService:
    """Service for VenueMapping operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, internal_venue_id: int) -> VenueMapping | None:
        """Get a mapping by internal venue ID."""
        return self.db.get(VenueMapping, internal_venue_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[VenueMapping]:
        """Get all venue mappings."""
        stmt = select(VenueMapping).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_external_id(self, external_id: str, data_source_id: int | None = None) -> list[VenueMapping]:
        """Get mappings by external venue ID."""
        stmt = select(VenueMapping).where(VenueMapping.external_venue_id == external_id)
        if data_source_id:
            stmt = stmt.where(VenueMapping.data_source_id == data_source_id)
        return list(self.db.scalars(stmt).all())


class QuoteMappingService:
    """Service for QuoteMapping operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, internal_quote_id: int) -> QuoteMapping | None:
        """Get a mapping by internal quote ID."""
        return self.db.get(QuoteMapping, internal_quote_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[QuoteMapping]:
        """Get all quote mappings."""
        stmt = select(QuoteMapping).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_external_id(self, external_id: str, data_source_id: int | None = None) -> list[QuoteMapping]:
        """Get mappings by external quote ID."""
        stmt = select(QuoteMapping).where(QuoteMapping.external_quote_id == external_id)
        if data_source_id:
            stmt = stmt.where(QuoteMapping.data_source_id == data_source_id)
        return list(self.db.scalars(stmt).all())
