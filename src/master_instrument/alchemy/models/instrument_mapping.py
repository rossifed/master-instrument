from sqlalchemy import String, UniqueConstraint, Index, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class InstrumentMapping(Base):
    __tablename__ = "instrument_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_instrument_id"),
        Index("idx_source_external_instrument_id", "source", "external_instrument_id"),
        {"schema": "ref_data"},
    )

    internal_instrument_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ref_data.instrument.instrument_id"),
        primary_key=True
    )
    external_instrument_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)

    def __repr__(self) -> str:
        return f"<InstrumentMapping(id={self.internal_instrument_id}, source={self.source}, external_id={self.external_instrument_id})>"
