"""Classification services."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import (
    ClassificationScheme,
    ClassificationNode,
    EntityClassification,
)


class ClassificationSchemeService:
    """Service for ClassificationScheme operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, scheme_id: int) -> ClassificationScheme | None:
        """Get a classification scheme by ID."""
        return self.db.get(ClassificationScheme, scheme_id)

    def get_all(self) -> list[ClassificationScheme]:
        """Get all classification schemes."""
        stmt = select(ClassificationScheme)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> ClassificationScheme | None:
        """Get classification scheme by mnemonic (e.g. 'GICS')."""
        stmt = select(ClassificationScheme).where(ClassificationScheme.mnemonic == mnemonic.upper())
        return self.db.scalar(stmt)


class ClassificationNodeService:
    """Service for ClassificationNode operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, node_id: int) -> ClassificationNode | None:
        """Get a classification node by ID."""
        return self.db.get(ClassificationNode, node_id)

    def get_all(self) -> list[ClassificationNode]:
        """Get all classification nodes."""
        stmt = (
            select(ClassificationNode)
            .order_by(ClassificationNode.classification_scheme_id, ClassificationNode.level_number, ClassificationNode.code)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_scheme(self, scheme_id: int) -> list[ClassificationNode]:
        """Get all nodes for a classification scheme."""
        stmt = (
            select(ClassificationNode)
            .where(ClassificationNode.classification_scheme_id == scheme_id)
            .order_by(ClassificationNode.level_number, ClassificationNode.code)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_level(self, scheme_id: int, level_number: int) -> list[ClassificationNode]:
        """Get nodes at a specific level."""
        stmt = (
            select(ClassificationNode)
            .where(
                ClassificationNode.classification_scheme_id == scheme_id,
                ClassificationNode.level_number == level_number
            )
            .order_by(ClassificationNode.code)
        )
        return list(self.db.scalars(stmt).all())

    def get_children(self, scheme_id: int, parent_code: str) -> list[ClassificationNode]:
        """Get child nodes of a parent."""
        stmt = (
            select(ClassificationNode)
            .where(
                ClassificationNode.classification_scheme_id == scheme_id,
                ClassificationNode.parent_code == parent_code
            )
            .order_by(ClassificationNode.code)
        )
        return list(self.db.scalars(stmt).all())

    def get_children_by_id(self, parent_node_id: int) -> list[ClassificationNode]:
        """Get child nodes by parent node ID."""
        parent = self.db.get(ClassificationNode, parent_node_id)
        if not parent:
            return []
        stmt = (
            select(ClassificationNode)
            .where(
                ClassificationNode.classification_scheme_id == parent.classification_scheme_id,
                ClassificationNode.parent_code == parent.code
            )
            .order_by(ClassificationNode.code)
        )
        return list(self.db.scalars(stmt).all())


class EntityClassificationService:
    """Service for EntityClassification operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_all(self) -> list[EntityClassification]:
        """Get all entity classifications."""
        stmt = select(EntityClassification)
        return list(self.db.scalars(stmt).all())

    def get_by_entity(self, entity_id: int) -> list[EntityClassification]:
        """Get all classifications for an entity."""
        stmt = (
            select(EntityClassification)
            .where(EntityClassification.entity_id == entity_id)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_scheme(self, scheme_id: int, skip: int = 0, limit: int = 100) -> list[EntityClassification]:
        """Get all entity classifications for a scheme with pagination."""
        stmt = (
            select(EntityClassification)
            .where(EntityClassification.classification_scheme_id == scheme_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_node(self, scheme_id: int, node_code: str, skip: int = 0, limit: int = 100) -> list[EntityClassification]:
        """Get all entities classified under a specific node."""
        stmt = (
            select(EntityClassification)
            .where(
                EntityClassification.classification_scheme_id == scheme_id,
                EntityClassification.classification_node_code == node_code
            )
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
