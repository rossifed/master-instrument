from sqlalchemy import String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Instrument(Base):
    __tablename__ = "instrument"
    __table_args__ = {"schema": "referential"}

    instrument_id: Mapped[int] = mapped_column(primary_key=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey("referential.entity.entity_id"))
    instrument_type_id: Mapped[int] = mapped_column(ForeignKey("referential.instrument_type.instrument_type_id"))
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    entity: Mapped["Entity"] = relationship(back_populates="instruments")
    instrument_type: Mapped["InstrumentType"] = relationship(back_populates="instruments")
    quotes: Mapped[list["Quote"]] = relationship(back_populates="instrument")