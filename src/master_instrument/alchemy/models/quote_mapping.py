from sqlalchemy import Index, String, UniqueConstraint,func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class QuoteMapping(Base):
    __tablename__ = "quote_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_quote_id"),
        Index("idx_source_external_quote_id", "source", "external_quote_id"),
        {"schema": "ref_data"},
    )

    internal_quote_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_quote_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


