from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from datetime import date

class Equity(Base):
    __tablename__ = "equity"
    __table_args__ = {"schema": "ref_data"}

    equity_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id"),primary_key=True)
    isin: Mapped[str] = mapped_column(String(12), nullable=True)
    cusip: Mapped[str] = mapped_column(String(9), nullable=True)
    sedol: Mapped[str] = mapped_column(String(7), nullable=True)
    ric: Mapped[str] = mapped_column(String(10), nullable=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=True)
    equity_type: Mapped[str] = mapped_column(String(1), nullable=False)
    description: Mapped[str] = mapped_column(String(100), nullable=False)
    div_unit: Mapped[str] = mapped_column(String(5), nullable=True)
    is_major: Mapped[bool] = mapped_column(nullable=False)
    country_id: Mapped[int] = mapped_column( ForeignKey("ref_data.country.country_id"),nullable=False)
    split_date: Mapped[date] = mapped_column(nullable=True)
    split_factor: Mapped[float] = mapped_column(nullable=True)
    entity: Mapped["Instrument"] = relationship(back_populates="equity", uselist=False)