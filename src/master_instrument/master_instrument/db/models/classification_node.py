from typing import Optional, TYPE_CHECKING
from sqlalchemy import (
    String,
    SmallInteger,
    ForeignKey,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
    Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .classification_level import ClassificationLevel

class ClassificationNode(Base):
    __tablename__ = "classification_node"
    __table_args__ = (
        PrimaryKeyConstraint(
            "classification_scheme_id",
            "code",
            name="pk_classification_node"
        ),
        ForeignKeyConstraint(
            ["classification_scheme_id", "parent_code"],
            ["master.classification_node.classification_scheme_id",
             "master.classification_node.code"],
            ondelete="CASCADE",
            name="fk_classification_node_parent"
        ),
        ForeignKeyConstraint(
            ["classification_scheme_id", "level_number"],
            ["master.classification_level.classification_scheme_id",
             "master.classification_level.level_number"],
            name="fk_classification_node_level"
        ),
        UniqueConstraint(
            "classification_scheme_id",
            "name",
            "level_number",
            name="uq_classification_node_scheme_id_name_level_number"
        ),
        Index(
            "idx_classification_node_scheme_id_parent_code",
            "classification_scheme_id",
            "parent_code"
        ),
        Index(
            "idx_classification_node_scheme_id_level_number",
            "classification_scheme_id",
            "level_number"
        ),
        {"schema": "master"},
    )

    classification_scheme_id: Mapped[int] = mapped_column(
        ForeignKey("master.classification_scheme.classification_scheme_id", ondelete="RESTRICT"),
        nullable=False
    )

    code: Mapped[str] = mapped_column(String(20), nullable=False)
    parent_code: Mapped[Optional[str]] = mapped_column(String(20))
    level_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    parent_node: Mapped[Optional["ClassificationNode"]] = relationship(
        "ClassificationNode",
        remote_side=[classification_scheme_id, code],
        lazy="selectin"
    )

    classification_level: Mapped["ClassificationLevel"] = relationship(
        "ClassificationLevel",
        lazy="selectin"
    )


    def __repr__(self) -> str:
        return (
            f"<ClassificationNode(scheme_id={self.classification_scheme_id}, "
            f"code={self.code}, name={self.name})>"
        )
