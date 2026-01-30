from typing import Optional, TYPE_CHECKING, List
from decimal import Decimal
import datetime
from sqlalchemy import String, Double, ForeignKey, Integer, Boolean, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from .instrument import Instrument
    from .venue import Venue
    from .total_return import TotalReturn

class Quote(Base):
    __tablename__ = "quote"
    __table_args__ = (
        UniqueConstraint("instrument_id", "venue_id", name="uq_quote_instrument_id_venue_id"),
        Index("idx_quote_ric", "ric"),
        Index("idx_quote_ticker", "ticker"),
        Index("idx_quote_mic", "mic"),
        Index("idx_quote_is_primary", "is_primary"),
        Index("idx_quote_instrument_id", "instrument_id"),
        Index("idx_quote_instrument_id_is_primary", "instrument_id", postgresql_where=text("is_primary = true")),
        {"schema": "master"},
    )

    quote_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("master.instrument.instrument_id", ondelete="CASCADE"))
    venue_id: Mapped[int] = mapped_column(ForeignKey("master.venue.venue_id", ondelete="RESTRICT"))
    currency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("master.currency.currency_id", ondelete="SET NULL"), nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    ticker: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    ric: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    mic: Mapped[Optional[str]] = mapped_column(String(12), nullable=True)
    market_name: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)   
    price_unit: Mapped[Optional[Decimal]] = mapped_column(Double, nullable=True)
    

    instrument: Mapped["Instrument"] = relationship(back_populates="quotes", lazy="selectin")
    venue: Mapped["Venue"] = relationship(back_populates="quotes", lazy="selectin")
    total_returns: Mapped[List["TotalReturn"]] = relationship(back_populates="quote", lazy="noload")

    def __repr__(self) -> str:
        return f"<Quote id={self.quote_id}, mic={self.mic}, primary={self.is_primary}, price_unit={self.price_unit}>"