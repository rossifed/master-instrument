from typing import Optional, TYPE_CHECKING
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .venue_type import VenueType
    from .country import Country

class Venue(Base):
    __tablename__ = "venue"
    __table_args__ = {"schema": "ref_data"}

    venue_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    mnemonic: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    venue_type_id: Mapped[int] = mapped_column(ForeignKey("ref_data.venue_type.venue_type_id"), nullable=False)
    country_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.country.country_id"), nullable=True)

    venue_type: Mapped["VenueType"] = relationship(back_populates="venues", lazy="selectin")
    country: Mapped[Optional["Country"]] = relationship(back_populates="venues", lazy="selectin")

    def __repr__(self) -> str:
        return f"<Venue(id={self.venue_id}, name={self.name})>"
