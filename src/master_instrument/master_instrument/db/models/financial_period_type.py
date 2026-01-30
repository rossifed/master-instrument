from typing import Optional
from sqlalchemy import String, SmallInteger, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class FinancialPeriodType(Base):
    __tablename__ = "financial_period_type"
    __table_args__ = (
        UniqueConstraint("name", name="uq_financial_period_type_name"),
        UniqueConstraint("mnemonic", name="uq_financial_period_type_mnemonic"),
        UniqueConstraint("months", name="uq_financial_period_type_months"),
        {"schema": "master"},
    )

    financial_period_type_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    mnemonic: Mapped[Optional[str]] = mapped_column(String(10))
    months: Mapped[Optional[int]] = mapped_column(SmallInteger)  # 12=Annual, 3=Quarterly, 6=Semi-Annual

    def __repr__(self):
        return f"<FinancialPeriodType(id={self.financial_period_type_id}, name={self.name}, months={self.months})>"
