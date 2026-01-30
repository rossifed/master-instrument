"""VenueType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import VenueType


class VenueTypeService:
    """Service for VenueType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, venue_type_id: int) -> VenueType | None:
        return self.db.get(VenueType, venue_type_id)

    def get_all(self) -> list[VenueType]:
        stmt = select(VenueType)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> VenueType | None:
        stmt = select(VenueType).where(VenueType.mnemonic == mnemonic.upper())
        return self.db.scalar(stmt)
