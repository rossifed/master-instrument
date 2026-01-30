from datetime import date
from typing import Optional

from sqlalchemy import (
    String,
    Date,
    Integer,
    SmallInteger,
    BigInteger,
    Double,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class CorpactEvent(Base):
    __tablename__ = "corpact_event"
    __table_args__ = (
        UniqueConstraint(
            "equity_id",
            "event_sequence",
            "corpact_type_id",
            "effective_date",
            name="uq_corpact_event_natural_key",
        ),
        Index("idx_corpact_event_equity_id", "equity_id"),
        Index("idx_corpact_event_effective_date", "effective_date"),
        Index("idx_corpact_event_equity_id_event_sequence", "equity_id", "event_sequence"),
        Index("idx_corpact_event_corpact_type_id", "corpact_type_id"),
        {"schema": "master"},
    )

    corpact_event_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )

    equity_id: Mapped[int] = mapped_column(
        ForeignKey("master.equity.equity_id", ondelete="CASCADE"),
        nullable=False,
    )

    event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)

    corpact_type_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.corpact_type.corpact_type_id", ondelete="RESTRICT"),
        nullable=False,
    )

    res_equity_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.equity.equity_id", ondelete="SET NULL"),
        nullable=True,
    )

    new_shares_count: Mapped[Optional[float]] = mapped_column(Double)
    old_shares_count: Mapped[Optional[float]] = mapped_column(Double)

    currency_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=True,
    )

    cash_amount: Mapped[Optional[float]] = mapped_column(Double)
 
    offer_company_name: Mapped[Optional[str]] = mapped_column(String(255))

    announced_date: Mapped[Optional[date]] = mapped_column(Date)
    record_date: Mapped[Optional[date]] = mapped_column(Date)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiry_date: Mapped[Optional[date]] = mapped_column(Date)

    equity = relationship("Equity", foreign_keys=[equity_id], lazy="selectin")
    res_equity = relationship(
        "Equity", foreign_keys=[res_equity_id], lazy="selectin"
    )
    corpact_type = relationship("CorpactType", lazy="selectin")
    currency = relationship("Currency", lazy="selectin")
