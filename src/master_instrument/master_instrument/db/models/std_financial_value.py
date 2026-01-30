from typing import Optional, TYPE_CHECKING
from datetime import date, datetime
from sqlalchemy import Numeric, ForeignKey, PrimaryKeyConstraint, UniqueConstraint, Index, Date, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .base import Base

if TYPE_CHECKING:
    from .std_financial_statement import StdFinancialStatement
    from .std_financial_item import StdFinancialItem
    from .financial_period_type import FinancialPeriodType
    from .financial_statement_type import FinancialStatementType
    from .company import Company


class StdFinancialValue(Base):
    """Standardized financial values with denormalized natural key for direct querying.
    
    The table uses composite PK on (std_financial_statement_id, std_financial_item_id) for MERGE.
    Natural key constraint ensures data integrity.
    Indexes optimized for signal queries by item + calendar_date.
    
    Natural key (source): Code, PerTypeCode, PerEndDt, StmtDt, StmtTypeCode, Item
    Mapped to: company_id, period_type_id, period_end_date, filing_end_date, statement_type_id, std_financial_item_id
    """
    __tablename__ = "std_financial_value"
    __table_args__ = (
        # Composite PK on surrogate keys (for MERGE operations)
        PrimaryKeyConstraint(
            "std_financial_statement_id",
            "std_financial_item_id",
            name="pk_std_financial_value",
        ),
        # Natural key constraint (data integrity)
        UniqueConstraint(
            "company_id",
            "period_type_id",
            "period_end_date",
            "filing_end_date",
            "statement_type_id",
            "std_financial_item_id",
            name="uq_std_financial_value_natural_key",
        ),
        # Signal queries: item + calendar_date (cross-company signals)
        Index(
            "idx_std_financial_value_item_id_calendar_end_date",
            "std_financial_item_id",
            "calendar_end_date",
        ),
        # Signal queries: company(s) + item + calendar_date
        Index(
            "idx_std_financial_value_company_id_item_id_calendar_end_date",
            "company_id",
            "std_financial_item_id",
            "calendar_end_date",
        ),
        {"schema": "master"},
    )

    # Composite PK part 1: FK to statement
    std_financial_statement_id: Mapped[int] = mapped_column(
        ForeignKey("master.std_financial_statement.std_financial_statement_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Denormalized natural key components for direct querying
    company_id: Mapped[int] = mapped_column(
        ForeignKey("master.company.company_id"),
        nullable=False,
    )
    period_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.financial_period_type.financial_period_type_id"),
        nullable=False,
    )
    period_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    filing_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    statement_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.financial_statement_type.financial_statement_type_id"),
        nullable=False,
    )
    # Composite PK part 2: FK to item
    std_financial_item_id: Mapped[int] = mapped_column(
        ForeignKey("master.std_financial_item.std_financial_item_id"),
        nullable=False,
    )

    # Denormalized for convenience (derived from filing)
    calendar_end_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Value
    value: Mapped[Optional[float]] = mapped_column(Numeric(20, 4))

    # Audit fields
    loaded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    company: Mapped["Company"] = relationship("Company")
    std_financial_statement: Mapped["StdFinancialStatement"] = relationship("StdFinancialStatement", back_populates="std_financial_values")
    std_financial_item: Mapped["StdFinancialItem"] = relationship("StdFinancialItem")
    period_type: Mapped["FinancialPeriodType"] = relationship("FinancialPeriodType")
    statement_type: Mapped["FinancialStatementType"] = relationship("FinancialStatementType")

    def __repr__(self):
        return (
            f"<StdFinancialValue("
            f"stmt_id={self.std_financial_statement_id}, "
            f"item_id={self.std_financial_item_id}, "
            f"company_id={self.company_id}, "
            f"period={self.period_end_date}, "
            f"value={self.value})>"
        )
