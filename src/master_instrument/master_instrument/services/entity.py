"""Entity service - simple CRUD."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Entity


class EntityService:
    """Service for Entity operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, entity_id: int) -> Entity | None:
        """Get a single entity by ID."""
        return self.db.get(Entity, entity_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Entity]:
        """Get all entities."""
        stmt = select(Entity).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def search(self, name: str, skip: int = 0, limit: int = 100) -> list[Entity]:
        """Search entities by name (ILIKE)."""
        stmt = (
            select(Entity)
            .where(Entity.name.ilike(f"%{name}%"))
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_type(self, entity_type_id: int, skip: int = 0, limit: int = 100) -> list[Entity]:
        """Get entities by type."""
        stmt = (
            select(Entity)
            .where(Entity.entity_type_id == entity_type_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
