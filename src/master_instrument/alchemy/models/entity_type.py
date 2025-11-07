from typing import List, TYPE_CHECKING
from sqlalchemy import String, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity

class EntityType(Base):
    __tablename__ = "entity_type"
    __table_args__ = (
        UniqueConstraint("mnemonic"),
        {"schema": "ref_data"},
    )

    entity_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    entities: Mapped[List["Entity"]] = relationship(back_populates="entity_type", lazy="selectin")

    def __repr__(self) -> str:
        return f"<EntityType(id={self.entity_type_id}, mnemonic={self.mnemonic})>"
