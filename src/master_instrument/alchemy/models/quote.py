from sqlalchemy import TIMESTAMP, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Quote(Base):
    __tablename__ = "quote"
    __table_args__ = {"schema": "ref_data"}

    quote_id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id"))
    venue_id: Mapped[int] = mapped_column(ForeignKey("ref_data.venue.venue_id"))

    instrument: Mapped["Instrument"] = relationship(back_populates="quotes")
