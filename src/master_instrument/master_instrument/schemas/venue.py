"""Venue Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class VenueTypeResponse(BaseModel):
    """VenueType response schema."""

    venue_type_id: int
    mnemonic: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class VenueResponse(BaseModel):
    """Venue response schema with venue_type."""

    venue_id: int
    name: str
    mnemonic: str
    venue_type: VenueTypeResponse

    model_config = ConfigDict(from_attributes=True)
