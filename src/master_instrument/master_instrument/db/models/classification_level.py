from typing import TYPE_CHECKING
from sqlalchemy import SmallInteger, String, ForeignKey, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .classification_scheme import ClassificationScheme

class ClassificationLevel(Base):
    __tablename__ = "classification_level"
    __table_args__ = (
        PrimaryKeyConstraint(
            "classification_scheme_id",
            "level_number",
            name="pk_classification_level"
        ),
        UniqueConstraint(
            "classification_scheme_id",
            "name",
            name="uq_classification_level_scheme_name"
        ),
        {"schema": "master"},
    )

    classification_scheme_id: Mapped[int] = mapped_column(
        ForeignKey("master.classification_scheme.classification_scheme_id", ondelete="RESTRICT"),
        nullable=False
    )

    level_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    classification_scheme: Mapped["ClassificationScheme"] = relationship(
        back_populates="classification_levels",
        lazy="selectin"
    )


    def __repr__(self) -> str:
        return (
            f"<ClassificationLevel(scheme_id={self.classification_scheme_id}, "
            f"level={self.level_number}, name={self.name})>"
        )
