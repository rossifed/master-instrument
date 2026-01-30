from typing import Optional
from sqlalchemy import String, Integer, SmallInteger, Boolean, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class StdFinancialItem(Base):
    __tablename__ = "std_financial_item"
    __table_args__ = (
        Index("idx_std_financial_item_name", "name"),
        {"schema": "master"},
    )

    # Using Refinitiv item IDs directly (no autoincrement)
    std_financial_item_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)

    
    # Business fields
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    statement_type_id: Mapped[Optional[int]] = mapped_column(
        SmallInteger, 
        ForeignKey("master.financial_statement_type.financial_statement_type_id", ondelete="SET NULL")
    )
    is_currency: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return f"<StdFinancialItem(id={self.std_financial_item_id}, name={self.name})>"
