from sqlalchemy import Index, String,Integer, UniqueConstraint,ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class QuoteMapping(Base):
    __tablename__ = "quote_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_quote_id"),
        Index("idx_source_external_quote_id", "source", "external_quote_id"),
        {"schema": "ref_data"},
    )

    internal_quote_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("ref_data.quote.quote_id"), primary_key=True
    )
    external_quote_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


