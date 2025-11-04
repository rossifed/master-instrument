from sqlalchemy import ForeignKey, String, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class Venue(Base):
    __tablename__ = "venue"
    __table_args__ = {"schema": "ref_data"}

    venue_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    mnemonic: Mapped[str] = mapped_column(String(50), nullable=True)
    venue_type_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.venue_type.venue_type_id"), nullable=False
    )
    country_id: Mapped[int | None] = mapped_column(
        ForeignKey("ref_data.country.country_id"), nullable=True
    )

    # relationships
    venue_type: Mapped["VenueType"] = relationship(back_populates="venues")
    country: Mapped["Country"] = relationship(back_populates="venues")
