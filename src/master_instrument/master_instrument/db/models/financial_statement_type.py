from typing import Optional
from sqlalchemy import String, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class FinancialStatementType(Base):
    __tablename__ = "financial_statement_type"
    __table_args__ = (
        UniqueConstraint("name", name="uq_financial_statement_type_name"),
        UniqueConstraint("mnemonic", name="uq_financial_statement_type_mnemonic"),
        {"schema": "master"},
    )

    financial_statement_type_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    mnemonic: Mapped[Optional[str]] = mapped_column(String(10))

    def __repr__(self):
        return f"<FinancialStatementType(id={self.financial_statement_type_id}, name={self.name})>"
