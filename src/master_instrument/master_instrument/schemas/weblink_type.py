"""WeblinkType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class WeblinkTypeResponse(BaseModel):
    """WeblinkType response schema."""
    weblink_type_id: int
    description: str
    model_config = ConfigDict(from_attributes=True)
