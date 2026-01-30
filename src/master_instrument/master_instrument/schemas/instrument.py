"""Instrument Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class InstrumentTypeRef(BaseModel):
    """Instrument type reference."""
    instrument_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class InstrumentResponse(BaseModel):
    """Instrument response schema - only fields from instrument table."""

    instrument_id: int
    entity_id: int
    instrument_type_id: int
    name: str
    symbol: str | None = None
    description: str | None = None
    
    # Relations
    entity: EntityRef | None = None
    instrument_type: InstrumentTypeRef | None = None

    model_config = ConfigDict(from_attributes=True)
