"""InstrumentType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class InstrumentTypeResponse(BaseModel):
    """InstrumentType response schema."""
    instrument_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)
