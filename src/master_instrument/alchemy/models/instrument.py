from typing import List, Optional, TYPE_CHECKING
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity
    from .instrument_type import InstrumentType
    from .quote import Quote

class Instrument(Base):
    __tablename__ = "instrument"
    __table_args__ = {"schema": "ref_data"}

    instrument_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey("ref_data.entity.entity_id"))
    instrument_type_id: Mapped[int] = mapped_column(ForeignKey("ref_data.instrument_type.instrument_type_id"))
    symbol: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(String(300), nullable=False)

    entity: Mapped["Entity"] = relationship(back_populates="instruments", uselist=False, lazy="selectin")
    instrument_type: Mapped["InstrumentType"] = relationship(back_populates="instruments", lazy="selectin")
    quotes: Mapped[List["Quote"]] = relationship(back_populates="instrument", lazy="selectin")

    def __repr__(self) -> str:
        return f"<Instrument(id={self.instrument_id}, symbol={self.symbol}, name={self.name})>"
