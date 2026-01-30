from typing import TYPE_CHECKING
from datetime import date, datetime
from sqlalchemy import (
    Date,
    SmallInteger,
    Double,
    ForeignKey,
    PrimaryKeyConstraint,
    Index,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .base import Base

if TYPE_CHECKING:
    from .currency import Currency


class FxRate(Base):
    """
    Historical FX rates timeseries table.
    Contains daily spot exchange rates for currency pairs.
    Partitioned by rate_date using TimescaleDB hypertables.
    """
    __tablename__ = "fx_rate"
    __table_args__ = (
        PrimaryKeyConstraint(
            "rate_date",
            "base_currency_id",
            "quote_currency_id",
            name="pk_fx_rate"
        ),
        Index("idx_fx_rate_base_currency_id_quote_currency_id", "base_currency_id", "quote_currency_id"),
        Index("idx_fx_rate_rate_date", "rate_date"),
        Index("idx_fx_rate_loaded_at", "loaded_at"),
        Index("idx_fx_rate_currencies_rate_date", "base_currency_id", "quote_currency_id", text("rate_date DESC")),
        Index("idx_fx_rate_base_currency_id_rate_date", "base_currency_id", "rate_date"),
        {"schema": "master"},
    )

    # Composite primary key (natural key)
    rate_date: Mapped[date] = mapped_column(Date, nullable=False)
    base_currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )
    quote_currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )

    # FX rate values
    mid_rate: Mapped[float | None] = mapped_column(Double, nullable=True)
    bid_rate: Mapped[float | None] = mapped_column(Double, nullable=True)
    ask_rate: Mapped[float | None] = mapped_column(Double, nullable=True)

    # Audit fields
    loaded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    base_currency: Mapped["Currency"] = relationship(
        foreign_keys=[base_currency_id],
        lazy="selectin"
    )
    quote_currency: Mapped["Currency"] = relationship(
        foreign_keys=[quote_currency_id],
        lazy="selectin"
    )

    def __repr__(self):
        return (
            f"<FxRate(date={self.rate_date}, base_ccy={self.base_currency_id}, "
            f"quote_ccy={self.quote_currency_id}, mid={self.mid_rate})>"
        )
