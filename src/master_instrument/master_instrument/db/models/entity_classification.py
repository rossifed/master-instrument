from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from .classification_node import ClassificationNode
    from .entity import Entity

class EntityClassification(Base):
    __tablename__ = "entity_classification"
    __table_args__ = (
        PrimaryKeyConstraint(
            "entity_id",
            "classification_scheme_id",
            name="pk_entity_classification",
        ),
        ForeignKeyConstraint(
            ["classification_scheme_id", "classification_node_code"],
            ["master.classification_node.classification_scheme_id",
             "master.classification_node.code"],
            name="fk_entity_classification_node",
            ondelete="CASCADE",
        ),
        {"schema": "master"},
    )

    entity_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity.entity_id", ondelete="CASCADE"),
        nullable=False
    )

    classification_scheme_id: Mapped[int] = mapped_column(
        ForeignKey("master.classification_scheme.classification_scheme_id", ondelete="CASCADE"),
        nullable=False
    )

    classification_node_code: Mapped[str] = mapped_column(String(20), nullable=False)

    classification_node: Mapped["ClassificationNode"] = relationship(lazy="selectin")
    entity: Mapped["Entity"] = relationship(lazy="selectin")


    def __repr__(self):
        return (
            f"<EntityClassification(entity_id={self.entity_id}, "
            f"scheme_id={self.classification_scheme_id}, "
            f"node_code={self.classification_node_code})>"
        )
