"""EntityType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class EntityTypeResponse(BaseModel):
    """EntityType response schema."""
    entity_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)
