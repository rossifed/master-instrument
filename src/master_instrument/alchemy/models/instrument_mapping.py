from sqlalchemy import TIMESTAMP, String, UniqueConstraint,func
from sqlalchemy.orm import Mapped, mapped_column
import datetime
from .base import Base


class InstrumentMapping(Base):
    __tablename__ = "instrument_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_instrument_id"),
        {"schema": "ref_data"},
    )

    internal_instrument_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_instrument_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


