from typing import List, TYPE_CHECKING
from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument

class InstrumentType(Base):
    __tablename__ = "instrument_type"
    __table_args__ = (
        UniqueConstraint("mnemonic"),
        {"schema": "ref_data"},
    )

    instrument_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)

    sys_period: Mapped[TSTZRANGE] = mapped_column(
        TSTZRANGE,
        nullable=False,
        server_default=text("tstzrange(current_timestamp, NULL)")
    )

    instruments: Mapped[List["Instrument"]] = relationship(back_populates="instrument_type", lazy="selectin")

    def __repr__(self) -> str:
        return f"<InstrumentType(id={self.instrument_type_id}, mnemonic={self.mnemonic})>"
