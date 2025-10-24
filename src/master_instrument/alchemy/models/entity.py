from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Entity(Base):
    __tablename__ = "entity"
    __table_args__ = {"schema": "referential"}

    entity_id: Mapped[int] = mapped_column(primary_key=True)
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    company: Mapped["Company"] = relationship(back_populates="entity", uselist=False)
    instruments: Mapped[list["Instrument"]] = relationship(back_populates="entity")
