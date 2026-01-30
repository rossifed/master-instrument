from typing import Optional, TYPE_CHECKING, List
from sqlalchemy import String, Integer, ForeignKey, Text, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity
    from .instrument_type import InstrumentType
    from .quote import Quote

class Instrument(Base):
    __tablename__ = "instrument"
    __table_args__ = (
        Index("idx_instrument_entity_id", "entity_id"),
        Index("idx_instrument_type_id", "instrument_type_id"),
        Index("idx_instrument_symbol", "symbol"),
        {"schema": "master"},
    )

    instrument_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    entity_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity.entity_id", ondelete="RESTRICT"),
        nullable=False
    )

    instrument_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.instrument_type.instrument_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    symbol: Mapped[Optional[str]] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    entity: Mapped["Entity"] = relationship(lazy="selectin")
    instrument_type: Mapped["InstrumentType"] = relationship(lazy="selectin")

    quotes: Mapped[List["Quote"]] = relationship(
        back_populates="instrument",
        lazy="noload"
    )


    def __repr__(self):
        return f"<Instrument(id={self.instrument_id}, symbol={self.symbol}, name={self.name})>"
