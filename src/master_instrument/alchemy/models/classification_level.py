from typing import TYPE_CHECKING
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .classification_system import ClassificationSystem

class ClassificationLevel(Base):
    __tablename__ = "classification_level"
    __table_args__ = {"schema": "ref_data"}

    classification_level_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    classification_system_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.classification_system.classification_system_id"),
        nullable=False
    )
    level: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    code: Mapped[str] = mapped_column(String(10), nullable=False)

    classification_system: Mapped["ClassificationSystem"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return f"<ClassificationLevel(id={self.classification_level_id}, code={self.code}, level={self.level})>"
