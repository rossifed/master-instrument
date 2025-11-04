from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class InstrumentType(Base):
    __tablename__ = "instrument_type"
    __table_args__ = (UniqueConstraint("mnemonic"),{"schema": "ref_data"})

    instrument_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)

    instruments: Mapped[list["Instrument"]] = relationship(back_populates="instrument_type")
