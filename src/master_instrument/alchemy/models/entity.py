from typing import List, TYPE_CHECKING
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .company import Company
    from .instrument import Instrument

class Entity(Base):
    __tablename__ = "entity"
    __table_args__ = {"schema": "ref_data"}

    entity_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_type_id: Mapped[int] = mapped_column(Integer, nullable=False)

    company: Mapped["Company"] = relationship(back_populates="entity", uselist=False, lazy="selectin")
    instruments: Mapped[List["Instrument"]] = relationship(back_populates="entity", lazy="selectin")

    def __repr__(self) -> str:
        return f"<Entity(id={self.entity_id}, name={self.name})>"
