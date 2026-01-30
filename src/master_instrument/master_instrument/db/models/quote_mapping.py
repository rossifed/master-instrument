from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .quote import Quote
    from .data_source import DataSource


class QuoteMapping(Base):
    __tablename__ = "quote_mapping"
    __table_args__ = (
        UniqueConstraint("data_source_id", "external_quote_id", name="uq_quote_mapping_source_ext"),
        {"schema": "master"},
    )

    internal_quote_id: Mapped[int] = mapped_column(
        ForeignKey("master.quote.quote_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_quote_id: Mapped[str] = mapped_column(String(100), nullable=False)

    # Relationships
    quote: Mapped["Quote"] = relationship(lazy="selectin")
    data_source: Mapped["DataSource"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return (
            f"<QuoteMapping(quote_id={self.internal_quote_id}, data_source_id={self.data_source_id}, "
            f"external_id={self.external_quote_id})>"
        )
