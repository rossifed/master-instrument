from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .instrument import Instrument
    from .data_source import DataSource
    from .instrument_type import InstrumentType


class InstrumentMapping(Base):
    """Maps external instrument IDs to internal instrument IDs.
    
    The unique constraint includes instrument_type_id because the same external_id
    could represent different instrument types from the same data source.
    """
    __tablename__ = "instrument_mapping"
    __table_args__ = (
        UniqueConstraint(
            "data_source_id", 
            "instrument_type_id",
            "external_instrument_id", 
            name="uq_instrument_mapping_ds_id_type_id_ext_id"
        ),
        # Index optimized for lookups: external_id is most selective
        Index(
            "idx_instrument_mapping_ext_id_ds_id_type_id",
            "external_instrument_id",
            "data_source_id",
            "instrument_type_id"
        ),
        {"schema": "master"},
    )

    internal_instrument_id: Mapped[int] = mapped_column(
        ForeignKey("master.instrument.instrument_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    instrument_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.instrument_type.instrument_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_instrument_id: Mapped[str] = mapped_column(String(100), nullable=False)

    # Relationships
    instrument: Mapped["Instrument"] = relationship(lazy="selectin")
    data_source: Mapped["DataSource"] = relationship(lazy="selectin")
    instrument_type: Mapped["InstrumentType"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return (
            f"<InstrumentMapping(instr_id={self.internal_instrument_id}, "
            f"type_id={self.instrument_type_id}, "
            f"data_source_id={self.data_source_id}, "
            f"external_id={self.external_instrument_id})>"
        )
