from datetime import date
from typing import Optional

from sqlalchemy import (
    String,
    Date,
    SmallInteger,
    Integer,
    BigInteger,
    Double,
    Boolean,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Dividend(Base):
    __tablename__ = "dividend"
    __table_args__ = (
        UniqueConstraint(
            "equity_id",
            "event_sequence",
            name="uq_dividend_natural_key",
        ),
        Index("idx_dividend_equity_id_effective_date", "equity_id", "effective_date"),
        Index("idx_dividend_currency_id", "currency_id"),
        Index("idx_dividend_dividend_type_id", "dividend_type_id"),
        {"schema": "master"},
    )

    dividend_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )

    equity_id: Mapped[int] = mapped_column(
        ForeignKey("master.equity.equity_id", ondelete="CASCADE"),
        nullable=False,
    )

    event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)

    dividend_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.dividend_type.dividend_type_id", ondelete="RESTRICT"),
        nullable=False,
    )

    dividend_rate: Mapped[float] = mapped_column(Double, nullable=False)

    pay_date: Mapped[Optional[date]] = mapped_column(Date)

    is_pay_date_estimated: Mapped[Optional[bool]] = mapped_column(Boolean)

    currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False,
    )

    effective_date: Mapped[date] = mapped_column(Date, nullable=False)

    is_effective_date_estimated: Mapped[Optional[bool]] = mapped_column(Boolean)

    equity = relationship("Equity", lazy="selectin")
    dividend_type = relationship("DividendType", lazy="selectin")
    currency = relationship("Currency", lazy="selectin")

    def __repr__(self):
        return f"<Dividend(id={self.dividend_id}, equity_id={self.equity_id}, seq={self.event_sequence}, rate={self.dividend_rate}, effective={self.effective_date})>"
