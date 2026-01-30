"""Region service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Region


class RegionService:
    """Service for Region operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, region_id: int) -> Region | None:
        return self.db.get(Region, region_id)

    def get_all(self) -> list[Region]:
        stmt = select(Region)
        return list(self.db.scalars(stmt).all())

    def get_by_code(self, code: str) -> Region | None:
        stmt = select(Region).where(Region.code == code.upper())
        return self.db.scalar(stmt)
