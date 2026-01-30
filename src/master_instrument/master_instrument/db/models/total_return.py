from typing import TYPE_CHECKING
from datetime import date, datetime
from sqlalchemy import (
    Date,
    Integer,
    Double,
    ForeignKey,
    Index,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .base import Base

if TYPE_CHECKING:
    from .quote import Quote


class TotalReturn(Base):
    """
    Total return index timeseries table.
    Contains end-of-day total return index values per quote.
    Partitioned by value_date using TimescaleDB hypertables.
    """
    __tablename__ = "total_return"
    __table_args__ = (
        Index("idx_total_return_quote_id_value_date", "quote_id", text("value_date DESC")),
        Index("idx_total_return_value_date", "value_date"),
        {"schema": "master"},
    )

    # Composite primary key (natural key)
    value_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    quote_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("master.quote.quote_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    )

    # Total return index value
    value: Mapped[float] = mapped_column(Double, nullable=False)

    # Audit fields
    loaded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False
    )

    # Relationships
    quote: Mapped["Quote"] = relationship(back_populates="total_returns")

    def __repr__(self) -> str:
        return (
            f"<TotalReturn(date={self.value_date}, quote_id={self.quote_id}, "
            f"value={self.value})>"
        )
