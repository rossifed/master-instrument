from sqlalchemy import Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class InstrumentMapping(Base):
    __tablename__ = "instrument_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_instrument_id"),
        Index("idx_source_external_instrument_id", "source", "external_instrument_id"),
        {"schema": "ref_data"},
    )

    internal_instrument_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_instrument_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


