from typing import Optional, TYPE_CHECKING
from datetime import date
from sqlalchemy import Date, Integer, SmallInteger, String, Boolean, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .company import Company
    from .financial_period_type import FinancialPeriodType
    from .currency import Currency
    from .std_financial_statement import StdFinancialStatement


class StdFinancialFiling(Base):
    __tablename__ = "std_financial_filing"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "period_end_date",
            "filing_end_date",
            "period_type_id",
            name="uq_std_financial_filing_company_period",
        ),
        Index(
            "idx_std_financial_filing_company_id",
            "company_id",
        ),
        Index(
            "idx_std_financial_filing_period_end_date",
            "period_end_date",
        ),
        Index(
            "idx_std_financial_filing_filing_end_date",
            "filing_end_date",
        ),
        {"schema": "master"},
    )

    std_financial_filing_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    company_id: Mapped[int] = mapped_column(
        ForeignKey("master.company.company_id", ondelete="CASCADE"),
        nullable=False,
    )
    period_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.financial_period_type.financial_period_type_id", ondelete="RESTRICT"),
        nullable=False,
    )
    reported_currency_id: Mapped[int] = mapped_column(
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False,
    )
    converted_currency_id: Mapped[int] = mapped_column(
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Period Information
    period_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    calendar_end_date: Mapped[Optional[date]] = mapped_column(Date)
    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer)
    fiscal_month: Mapped[Optional[int]] = mapped_column(SmallInteger)
    is_interim: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_hybrid: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Filing Information
    filing_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    is_final: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    announcement_date: Mapped[Optional[date]] = mapped_column(Date)

    # Units (scale factors: 1, 1000, 1000000 for units/thousands/millions)
    reported_unit: Mapped[Optional[str]] = mapped_column(String(10))
    converted_unit: Mapped[Optional[str]] = mapped_column(String(10))

    # Relationships
    company: Mapped["Company"] = relationship("Company", back_populates="std_financial_filings")
    period_type: Mapped["FinancialPeriodType"] = relationship("FinancialPeriodType")
    reported_currency: Mapped["Currency"] = relationship("Currency", foreign_keys=[reported_currency_id])
    converted_currency: Mapped["Currency"] = relationship("Currency", foreign_keys=[converted_currency_id])
    std_financial_statements: Mapped[list["StdFinancialStatement"]] = relationship(
        back_populates="std_financial_filing",
        lazy="noload"
    )

    def __repr__(self):
        return (
            f"<StdFinancialFiling("
            f"id={self.std_financial_filing_id}, "
            f"company_id={self.company_id}, "
            f"period_end={self.period_end_date}, "
            f"filing_end={self.filing_end_date})>"
        )
