from sqlalchemy import TIMESTAMP, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Venue(Base):
    __tablename__ = "venue"
    __table_args__ = {"schema": "master"}

    venue_id: Mapped[int] = mapped_column(primary_key=True)
    instrument_id: Mapped[int] = mapped_column()
    venue_type_id: Mapped[int] = mapped_column(ForeignKey("master.venue_type.venue_type_id"))
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    venue_type: Mapped["VenueType"] = relationship(back_populates="venues")
