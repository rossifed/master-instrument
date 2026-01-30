from typing import List, Optional, TYPE_CHECKING
from sqlalchemy import Integer, String, ForeignKey, Text, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument
    from .entity_type import EntityType

class Entity(Base):
    __tablename__ = "entity"
    __table_args__ = (
        Index("idx_entity_name", "name"),
        Index("idx_entity_type_id", "entity_type_id"),
        {"schema": "master"},
    )

    entity_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    entity_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity_type.entity_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    entity_type: Mapped["EntityType"] = relationship(lazy="selectin")

    instruments: Mapped[List["Instrument"]] = relationship(
        back_populates="entity",
        lazy="noload"
    )


    def __repr__(self) -> str:
        return f"<Entity(id={self.entity_id}, name={self.name})>"
