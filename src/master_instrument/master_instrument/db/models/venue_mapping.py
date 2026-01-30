from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .venue import Venue
    from .data_source import DataSource
    from .venue_type import VenueType


class VenueMapping(Base):
    """Maps external venue IDs to internal venue IDs.
    
    The unique constraint includes venue_type_id because the same external_id
    could represent different venue types from the same data source.
    """
    __tablename__ = "venue_mapping"
    __table_args__ = (
        UniqueConstraint(
            "data_source_id", 
            "venue_type_id",
            "external_venue_id", 
            name="uq_venue_mapping_ds_id_type_id_ext_id"
        ),
        # Index optimized for lookups: external_id is most selective
        Index(
            "idx_venue_mapping_ext_id_ds_id_type_id",
            "external_venue_id",
            "data_source_id",
            "venue_type_id"
        ),
        {"schema": "master"},
    )

    internal_venue_id: Mapped[int] = mapped_column(
        ForeignKey("master.venue.venue_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    venue_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.venue_type.venue_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_venue_id: Mapped[str] = mapped_column(String(100), nullable=False)

    # Relationships
    venue: Mapped["Venue"] = relationship(lazy="selectin")
    data_source: Mapped["DataSource"] = relationship(lazy="selectin")
    venue_type: Mapped["VenueType"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return (
            f"<VenueMapping(venue_id={self.internal_venue_id}, "
            f"type_id={self.venue_type_id}, "
            f"data_source_id={self.data_source_id}, "
            f"external_id={self.external_venue_id})>"
        )
