from typing import Optional
from sqlalchemy import String, Integer, Text, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class ClassificationSystem(Base):
    __tablename__ = "classification_system"
    __table_args__ = {"schema": "ref_data"}

    classification_system_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_type_id: Mapped[int] = mapped_column(ForeignKey("ref_data.entity_type.entity_type_id"), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<ClassificationSystem(id={self.classification_system_id}, mnemonic={self.mnemonic})>"
