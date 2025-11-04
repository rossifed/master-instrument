from sqlalchemy import String, UniqueConstraint,Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class VenueMapping(Base):
    __tablename__ = "venue_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_venue_id"),
        Index("idx__source_external_venue_id", "source", "external_venue_id"),
        {"schema": "ref_data"},
    )

    internal_venue_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_venue_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


