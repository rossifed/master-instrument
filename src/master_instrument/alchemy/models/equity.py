from sqlalchemy import String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Equity(Base):
    __tablename__ = "equity"
    __table_args__ = {"schema": "ref_data"}

    equity_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument.instrument_id"),primary_key=True)
    isin: Mapped[str] = mapped_column(String(12), nullable=True)

    entity: Mapped["Instrument"] = relationship(back_populates="equity")