from typing import Optional, TYPE_CHECKING, List
from datetime import date
from sqlalchemy import String, ForeignKey, Boolean, Float, Index, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument
    from .equity_type import EquityType
    from .country import Country
    from .share_outstanding import ShareOutstanding

class Equity(Base):
    __tablename__ = "equity"
    __table_args__ = (
        Index("idx_equity_isin", "isin"),
        Index("idx_equity_cusip", "cusip"),
        Index("idx_equity_sedol", "sedol"),
        Index("idx_equity_ric", "ric"),
        Index("idx_equity_ticker", "ticker"),
        Index("idx_equity_type_id", "equity_type_id"),
        {"schema": "master"},
    )

    equity_id: Mapped[int] = mapped_column(
        ForeignKey("master.instrument.instrument_id", ondelete="CASCADE"),
        primary_key=True
    )

    security_id: Mapped[int] = mapped_column(Integer, nullable=False)

    isin: Mapped[Optional[str]] = mapped_column(String(12))
    cusip: Mapped[Optional[str]] = mapped_column(String(9))
    sedol: Mapped[Optional[str]] = mapped_column(String(7))
    ric: Mapped[Optional[str]] = mapped_column(String(20))
    ticker: Mapped[Optional[str]] = mapped_column(String(20))

    delisted_date: Mapped[Optional[date]]

    equity_type_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.equity_type.equity_type_id")
    )

    issue_type: Mapped[Optional[str]] = mapped_column(String(1))
    issue_description: Mapped[Optional[str]] = mapped_column(String(100))
    div_unit: Mapped[Optional[str]] = mapped_column(String(5))

    is_major_security: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false"
    )

    country_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.country.country_id")
    )

    split_date: Mapped[Optional[date]]
    split_factor: Mapped[Optional[float]] = mapped_column(Float)

    instrument: Mapped["Instrument"] = relationship(
        uselist=False,
        lazy="selectin",
        passive_deletes=True,
    )

    equity_type: Mapped[Optional["EquityType"]] = relationship(
        lazy="selectin"
    )

    country: Mapped[Optional["Country"]] = relationship(
        lazy="selectin"
    )

    share_outstandings: Mapped[List["ShareOutstanding"]] = relationship(
        back_populates="equity",
        lazy="noload"
    )


    @property
    def name(self) -> str:
        return self.instrument.name

    @property
    def description(self) -> Optional[str]:
        return self.instrument.description

    @property
    def symbol(self) -> Optional[str]:
        return self.instrument.symbol

    def __repr__(self):
        return f"<Equity(id={self.equity_id}, isin={self.isin}, ticker={self.ticker})>"
