from sqlalchemy import TIMESTAMP, ForeignKey,String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Venue(Base):
    __tablename__ = "venue"
    __table_args__ = {"schema": "ref_data"}

    venue_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    venue_type_id: Mapped[int] = mapped_column(ForeignKey("ref_data.venue_type.venue_type_id"))

    venue_type: Mapped["VenueType"] = relationship(back_populates="venues")
