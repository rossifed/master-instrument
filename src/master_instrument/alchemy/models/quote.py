from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
import datetime

class Quote(Base):
    __tablename__ = "quote"
    __table_args__ = {"schema": "ref_data"}

    quote_id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id"))
    venue_id: Mapped[int] = mapped_column(ForeignKey("ref_data.venue.venue_id"))
    delisted_date: Mapped[datetime.date] = mapped_column(nullable=True)
    currency: Mapped[str] = mapped_column(nullable=False)
    mic : Mapped[str] = mapped_column(nullable=True)
    price_unit: Mapped[str] = mapped_column(nullable=False)
    is_primary: Mapped[bool] = mapped_column(nullable=False, default=False)
    instrument: Mapped["Instrument"] = relationship(back_populates="quotes")
    venue: Mapped["Venue"] = relationship(back_populates="quotes")
