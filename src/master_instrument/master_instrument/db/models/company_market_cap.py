from typing import TYPE_CHECKING, Optional
from datetime import date, datetime
from sqlalchemy import (
    Date,
    BigInteger,
    Double,
    ForeignKey,
    Index,
    SmallInteger,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .base import Base

if TYPE_CHECKING:
    from .company import Company
    from .currency import Currency


class CompanyMarketCap(Base):
    """
    Historical company market capitalization timeseries table.
    Contains daily market cap and shares outstanding.
    Partitioned by valuation_date using TimescaleDB hypertables.
    """
    __tablename__ = "company_market_cap"
    __table_args__ = (
        Index("idx_company_market_cap_company_date", "company_id", "valuation_date"),
        Index("idx_company_market_cap_date", "valuation_date"),
        Index("idx_company_market_cap_loaded_at", "loaded_at"),
        {"schema": "master"},
    )

    # Composite primary key (natural key)
    valuation_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    company_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("master.company.company_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    )
    currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )

    # Market capitalization and shares
    market_cap: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    shares_outstanding: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    # Audit fields
    loaded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    company: Mapped["Company"] = relationship(lazy="selectin")  # type: ignore
    currency: Mapped["Currency"] = relationship(lazy="selectin")  # type: ignore

    def __repr__(self):
        return (
            f"<CompanyMarketCap(date={self.valuation_date}, company_id={self.company_id}, "
            f"currency_id={self.currency_id}, market_cap={self.market_cap}, shares={self.shares_outstanding})>"
        )
