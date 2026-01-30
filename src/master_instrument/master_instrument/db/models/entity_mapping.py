from typing import TYPE_CHECKING
from sqlalchemy import String, UniqueConstraint, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity
    from .data_source import DataSource
    from .entity_type import EntityType


class EntityMapping(Base):
    """Maps external entity IDs to internal entity IDs.
    
    The unique constraint includes entity_type_id because the same external_id
    could represent different entity types from the same data source.
    """
    __tablename__ = "entity_mapping"
    __table_args__ = (
        UniqueConstraint(
            "data_source_id", 
            "entity_type_id",
            "external_entity_id", 
            name="uq_entity_mapping_ds_id_type_id_ext_id"
        ),
        # Index optimized for lookups: external_id is most selective
        Index(
            "idx_entity_mapping_ext_id_ds_id_type_id",
            "external_entity_id",
            "data_source_id",
            "entity_type_id"
        ),
        {"schema": "master"},
    )

    internal_entity_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity.entity_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    entity_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity_type.entity_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_entity_id: Mapped[str] = mapped_column(String(100), nullable=False)

    # Relationships
    entity: Mapped["Entity"] = relationship(lazy="selectin")
    data_source: Mapped["DataSource"] = relationship(lazy="selectin")
    entity_type: Mapped["EntityType"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return (
            f"<EntityMapping(entity_id={self.internal_entity_id}, "
            f"type_id={self.entity_type_id}, "
            f"data_source_id={self.data_source_id}, "
            f"external_id={self.external_entity_id})>"
        )
