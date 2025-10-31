from sqlalchemy import TIMESTAMP, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Quote(Base):
    __tablename__ = "quote"
    __table_args__ = {"schema": "master"}

    quote_id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("master.instrument.instrument_id"))
    venue_id: Mapped[int] = mapped_column(ForeignKey("master.venue.venue_id"))
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    instrument: Mapped["Instrument"] = relationship(back_populates="quotes")
