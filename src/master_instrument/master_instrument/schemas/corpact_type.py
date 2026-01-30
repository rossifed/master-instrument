"""CorpactType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class CorpactTypeResponse(BaseModel):
    """CorpactType response schema."""
    corpact_type_id: int
    code: str
    description: str
    model_config = ConfigDict(from_attributes=True)
