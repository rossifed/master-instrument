from typing import Optional
from sqlalchemy import String, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

class ClassificationScheme(Base):
    __tablename__ = "classification_scheme"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_classification_scheme_mnemonic"),
        UniqueConstraint("name", name="uq_classification_scheme_name"),
        {"schema": "master"},
    )

    classification_scheme_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(200))

    classification_levels = relationship(
        "ClassificationLevel",
        back_populates="classification_scheme",
        lazy="noload"
    )


    def __repr__(self) -> str:
        return (
            f"<ClassificationScheme(id={self.classification_scheme_id}, "
            f"mnemonic={self.mnemonic})>"
        )
