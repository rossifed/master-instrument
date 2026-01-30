"""Entity Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class EntityTypeRef(BaseModel):
    """Entity type reference."""
    entity_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityResponse(BaseModel):
    """Entity response schema."""
    entity_id: int
    name: str
    description: str | None = None
    entity_type: EntityTypeRef | None = None
    model_config = ConfigDict(from_attributes=True)
