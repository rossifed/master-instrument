"""Classification Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class ClassificationSchemeResponse(BaseModel):
    """Classification scheme response."""
    classification_scheme_id: int
    mnemonic: str
    name: str
    description: str | None = None
    model_config = ConfigDict(from_attributes=True)


class ClassificationNodeResponse(BaseModel):
    """Classification node response."""
    classification_scheme_id: int
    code: str
    name: str
    level_number: int
    parent_code: str | None = None
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference for classification."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityClassificationResponse(BaseModel):
    """Entity classification response."""
    entity_id: int
    classification_scheme_id: int
    classification_node_code: str
    entity: EntityRef | None = None
    classification_node: ClassificationNodeResponse | None = None
    model_config = ConfigDict(from_attributes=True)
