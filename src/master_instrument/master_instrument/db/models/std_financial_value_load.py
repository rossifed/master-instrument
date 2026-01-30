from sqlalchemy import Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, CDCLoadMixin


class StdFinancialValueLoad(CDCLoadMixin, Base):
    """Tracks CDC load operations for std_financial_value table."""
    
    __tablename__ = "std_financial_value_load"
    __table_args__ = (
        Index("idx_std_financial_value_load_time", "loaded_at"),
        {"schema": "master"},
    )

    std_financial_value_load_id: Mapped[int] = mapped_column(
        Integer, 
        primary_key=True, 
        autoincrement=True
    )

    def __repr__(self) -> str:
        return (
            f"<StdFinancialValueLoad(id={self.std_financial_value_load_id}, "
            f"version={self.last_source_version}, "
            f"at={self.loaded_at})>"
        )
