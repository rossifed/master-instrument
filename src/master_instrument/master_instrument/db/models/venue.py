from typing import Optional, TYPE_CHECKING, List
from sqlalchemy import String, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .venue_type import VenueType
    from .country import Country
    from .quote import Quote

class Venue(Base):
    __tablename__ = "venue"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_venue_mnemonic"),
        {"schema": "master"},
    )

    venue_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)

    venue_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.venue_type.venue_type_id"),
        nullable=False
    )

    country_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.country.country_id")
    )

    venue_type: Mapped["VenueType"] = relationship(
        lazy="selectin"
    )

    country: Mapped[Optional["Country"]] = relationship(
        lazy="selectin"
    )

    quotes: Mapped[List["Quote"]] = relationship(
        back_populates="venue",
        lazy="noload"
    )


    def __repr__(self):
        return f"<Venue(id={self.venue_id}, mnemonic={self.mnemonic})>"
