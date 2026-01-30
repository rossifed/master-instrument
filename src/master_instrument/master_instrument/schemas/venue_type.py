"""VenueType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class VenueTypeResponse(BaseModel):
    """VenueType response schema."""
    venue_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)
