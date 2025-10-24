from sqlalchemy import String, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class VenueType(Base):
    __tablename__ = "venue_type"
    __table_args__ = {"schema": "referential"}

    venue_type_id: Mapped[int] = mapped_column(primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    venues: Mapped[list["Venue"]] = relationship(back_populates="venue_type")
