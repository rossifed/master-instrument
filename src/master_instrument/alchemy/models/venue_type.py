from typing import List, TYPE_CHECKING
from sqlalchemy import String, UniqueConstraint, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .venue import Venue

class VenueType(Base):
    __tablename__ = "venue_type"
    __table_args__ = (
        UniqueConstraint("mnemonic"),
        {"schema": "ref_data"},
    )

    venue_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    venues: Mapped[List["Venue"]] = relationship(back_populates="venue_type", lazy="selectin")

    def __repr__(self) -> str:
        return f"<VenueType(id={self.venue_type_id}, mnemonic={self.mnemonic})>"
