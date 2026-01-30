from typing import Optional
from sqlalchemy import String, UniqueConstraint, Index, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text

class StdFinancialItemMapping(Base):
    __tablename__ = "std_financial_item_mapping"
    __table_args__ = (
        UniqueConstraint("data_source_id", "external_item_id", name="uq_std_financial_item_mapping_data_source_id_external_item_id"),
        Index("idx_std_financial_item_mapping_external_item_id_data_source_id", "external_item_id", "data_source_id"),  # For JOIN lookups
        {"schema": "master"},
    )

    internal_item_id: Mapped[int] = mapped_column(
        ForeignKey("master.std_financial_item.std_financial_item_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_item_id: Mapped[str] = mapped_column(String(100), nullable=False)
    
    # Provider-specific metadata
    # RKD schema: {"coa": str, "level_": str, "agg": int, "item_precision": int, "line_id": int, "adjust": int}
    provider_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)

    def __repr__(self) -> str:
        return (
            f"<StdFinancialItemMapping(item_id={self.internal_item_id}, "
            f"data_source_id={self.data_source_id}, external_id={self.external_item_id})>"
        )
