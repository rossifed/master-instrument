from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import String, Date, BigInteger, Double, TIMESTAMP, ForeignKey, UniqueConstraint, Index, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from .base import Base


class CorpactAdjustment(Base):
    __tablename__ = "corpact_adjustment"
    __table_args__ = (
        Index('idx_corpact_adjustment_equity_id_adj_date_end_adj_date', 'equity_id', 'adj_date', 'end_adj_date'),
        {'schema': 'master'}
    )
    
    equity_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('master.equity.equity_id', ondelete='CASCADE'), primary_key=True)
    adj_date: Mapped[date] = mapped_column(Date, primary_key=True)
    adj_type: Mapped[int] = mapped_column(
        SmallInteger, 
        ForeignKey('master.corpact_type.corpact_type_id', ondelete='RESTRICT'),
        primary_key=True
    )
    
    end_adj_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    adj_factor: Mapped[Decimal] = mapped_column(Double, nullable=False)
    cum_adj_factor: Mapped[Decimal] = mapped_column(Double, nullable=False)

