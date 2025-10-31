from sqlalchemy import TIMESTAMP, String, UniqueConstraint,func
from sqlalchemy.orm import Mapped, mapped_column
import datetime
from .base import Base


class InstrumentIdMapping(Base):
    __tablename__ = "instrument_id_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_instrument_id"),
        {"schema": "master"},
    )

    internal_instrument_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_instrument_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    valid_from: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    valid_to: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

