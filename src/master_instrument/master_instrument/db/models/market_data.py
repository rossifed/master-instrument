from typing import Optional, TYPE_CHECKING
from datetime import date, datetime
from sqlalchemy import (
    Date,
    BigInteger,
    Integer,
    Double,
    ForeignKey,
    Index,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .base import Base

if TYPE_CHECKING:
    from .quote import Quote
    from .currency import Currency


class MarketData(Base):
    """
    Raw market data timeseries table.
    Contains end-of-day OHLCV prices, bid/ask spreads, and VWAP.
    NULLs are preserved - no forward-filling in this table.
    Partitioned by trade_date using TimescaleDB hypertables.
    """
    __tablename__ = "market_data"
    __table_args__ = (
        Index("idx_market_data_quote_id_trade_date", "quote_id", text("trade_date DESC")),
        Index("idx_market_data_quote_id_trade_date_notnull", "quote_id", text("trade_date DESC"), postgresql_where=text("close IS NOT NULL")),
        Index("idx_market_data_trade_date", "trade_date"),
        {"schema": "master"},
    )

    # Composite primary key (natural key)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    quote_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("master.quote.quote_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    )

    # Foreign keys
    currency_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )

    # OHLC prices
    open: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    close: Mapped[Optional[float]] = mapped_column(Double, nullable=True)

    # Volume
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    # Bid/Ask spreads
    bid: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(Double, nullable=True)

    # Volume-weighted average price
    vwap: Mapped[Optional[float]] = mapped_column(Double, nullable=True)

    # Audit fields
    loaded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    quote: Mapped["Quote"] = relationship(lazy="selectin")  # type: ignore
    currency: Mapped["Currency"] = relationship(lazy="selectin")  # type: ignore

    def __repr__(self):
        return (
            f"<MarketData(date={self.trade_date}, quote_id={self.quote_id}, "
            f"close={self.close}, volume={self.volume})>"
        )
