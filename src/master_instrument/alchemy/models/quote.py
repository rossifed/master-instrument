from typing import Optional, TYPE_CHECKING
from decimal import Decimal
import datetime
from sqlalchemy import String, Numeric, ForeignKey, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument
    from .venue import Venue

class Quote(Base):
    __tablename__ = "quote"
    __table_args__ = {"schema": "ref_data"}

    quote_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id", ondelete="CASCADE"))
    venue_id: Mapped[int] = mapped_column(ForeignKey("ref_data.venue.venue_id"))
    currency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.currency.currency_id"), nullable=True)
    delisted_date: Mapped[Optional[datetime.date]] = mapped_column(nullable=True)
    mic: Mapped[Optional[str]] = mapped_column(String(12), nullable=True)
    market_name: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    price_unit: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6), nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    instrument: Mapped["Instrument"] = relationship(back_populates="quotes", lazy="selectin")
    venue: Mapped["Venue"] = relationship(back_populates="quotes", lazy="selectin")

    def __repr__(self) -> str:
        return f"<Quote id={self.quote_id}, mic={self.mic}, primary={self.is_primary}, price_unit={self.price_unit}>"
