from typing import Optional, TYPE_CHECKING, List
from datetime import date
from sqlalchemy import Date, Integer, String, Boolean, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .std_financial_filing import StdFinancialFiling
    from .financial_statement_type import FinancialStatementType
    from .std_financial_value import StdFinancialValue


class StdFinancialStatement(Base):
    __tablename__ = "std_financial_statement"
    __table_args__ = (
        UniqueConstraint(
            "std_financial_filing_id",
            "statement_type_id",
            name="uq_std_financial_statement_filing_id_statement_type_id",
        ),
        Index(
            "idx_std_financial_statement_std_financial_filing_id",
            "std_financial_filing_id",
        ),
        Index(
            "idx_std_financial_statement_statement_type_id",
            "statement_type_id",
        ),
        {"schema": "master"},
    )

    std_financial_statement_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    std_financial_filing_id: Mapped[int] = mapped_column(
        ForeignKey("master.std_financial_filing.std_financial_filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    statement_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.financial_statement_type.financial_statement_type_id"),
        nullable=False,
    )
    # accounting_standard_id: Mapped[Optional[int]] = mapped_column(
    #     ForeignKey("master.accounting_standard.accounting_standard_id")
    # )

    # Statement Properties
    is_complete: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_consolidated: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    
    
    # Dates
    public_date: Mapped[Optional[date]] = mapped_column(Date)
    last_update_date: Mapped[Optional[date]] = mapped_column(Date)
    
    # Source
    source: Mapped[Optional[str]] = mapped_column(String(50))

    # Relationships
    std_financial_filing: Mapped["StdFinancialFiling"] = relationship("StdFinancialFiling", back_populates="std_financial_statements")
    statement_type: Mapped["FinancialStatementType"] = relationship("FinancialStatementType")
    std_financial_values: Mapped[List["StdFinancialValue"]] = relationship("StdFinancialValue", back_populates="std_financial_statement")

    def __repr__(self):
        return (
            f"<StdFinancialStatement("
            f"id={self.std_financial_statement_id}, "
            f"filing_id={self.std_financial_filing_id}, "
            f"type_id={self.statement_type_id})>"
        )
