from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, BigInteger, SmallInteger, Double, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class DividendAdjustment(Base):
    __tablename__ = "dividend_adjustment"
    __table_args__ = (
        UniqueConstraint('equity_id', 'ex_div_date', name='uq_dividend_adjustment_equity_id_ex_div_date'),
        {'schema': 'master'}
    )
    
    equity_id: Mapped[int] = mapped_column(
        BigInteger, 
        ForeignKey('master.equity.equity_id', ondelete='CASCADE'),
        primary_key=True
    )
    ex_div_date: Mapped[date] = mapped_column(Date, primary_key=True)
    
    price_date: Mapped[date] = mapped_column(Date, nullable=False)
    close: Mapped[Decimal] = mapped_column(Double, nullable=False)
    price_currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey('master.currency.currency_id', ondelete='RESTRICT'),
        nullable=False
    )
    
    total_div_converted: Mapped[Decimal] = mapped_column(Double, nullable=False)
    div_adj_factor: Mapped[Decimal] = mapped_column(Double, nullable=False)
    cum_div_factor: Mapped[Decimal] = mapped_column(Double, nullable=False)
    
    equity = relationship("Equity", lazy="selectin")
    price_currency = relationship("Currency", lazy="selectin")

    def __repr__(self):
        return f"<DividendAdjustment(equity_id={self.equity_id}, ex_div_date={self.ex_div_date}, cum_factor={self.cum_div_factor})>"
