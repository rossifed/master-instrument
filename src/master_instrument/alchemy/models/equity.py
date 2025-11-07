from typing import Optional, TYPE_CHECKING
from datetime import date
from sqlalchemy import String, SmallInteger, ForeignKey, Integer, Boolean, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument
    from .equity_type import EquityType
    from .country import Country

class Equity(Base):
    __tablename__ = "equity"
    __table_args__ = {"schema": "ref_data"}

    equity_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id"), primary_key=True)
    isin: Mapped[Optional[str]] = mapped_column(String(12), nullable=True)
    cusip: Mapped[Optional[str]] = mapped_column(String(9), nullable=True)
    sedol: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ric: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    ticker: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    delistd_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    equity_type_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.equity_type.equity_type_id"), nullable=True)
    issue_type: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    issue_description: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    div_unit: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    is_major_security: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_primary_country: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    country_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.country.country_id"), nullable=True)
    split_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    split_factor: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    entity: Mapped["Instrument"] = relationship(back_populates="equity", uselist=False, lazy="selectin")
    equity_type: Mapped[Optional["EquityType"]] = relationship(back_populates="instruments", lazy="selectin")

    def __repr__(self) -> str:
        return f"<Equity(id={self.equity_id}, isin={self.isin}, ticker={self.ticker})>"
