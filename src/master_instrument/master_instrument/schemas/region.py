"""Region Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class RegionResponse(BaseModel):
    """Region response schema."""
    region_id: int
    code: str
    name: str
    model_config = ConfigDict(from_attributes=True)
