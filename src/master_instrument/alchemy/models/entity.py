from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Entity(Base):
    __tablename__ = "entity"
    __table_args__ = {"schema": "ref_data"}

    entity_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    entity_type_id: Mapped[int] = mapped_column(nullable=False)

    company: Mapped["Company"] = relationship(back_populates="entity", uselist=False)
    instruments: Mapped[list["Instrument"]] = relationship(back_populates="entity")
