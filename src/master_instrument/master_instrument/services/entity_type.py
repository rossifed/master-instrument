"""EntityType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import EntityType


class EntityTypeService:
    """Service for EntityType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, entity_type_id: int) -> EntityType | None:
        return self.db.get(EntityType, entity_type_id)

    def get_all(self) -> list[EntityType]:
        stmt = select(EntityType)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> EntityType | None:
        stmt = select(EntityType).where(EntityType.mnemonic == mnemonic.upper())
        return self.db.scalar(stmt)
