from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Index, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from .base import Base

if TYPE_CHECKING:
    from .equity import Equity


class ShareOutstanding(Base):
    __tablename__ = "share_outstanding"
    __table_args__ = (
        PrimaryKeyConstraint("date", "equity_id", name="pk_share_outstanding"),
        Index("idx_share_outstanding_equity_id_date", "equity_id", "date"),
        {"schema": "master"},
    )

    date: Mapped[Date] = mapped_column(Date, nullable=False)
    equity_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("master.equity.equity_id", ondelete="CASCADE"),
        nullable=False,
    )
    number_of_shares: Mapped[int] = mapped_column(BigInteger, nullable=False)
    loaded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        nullable=False
    )

    # Relationship
    equity: Mapped["Equity"] = relationship("Equity", back_populates="share_outstandings")
