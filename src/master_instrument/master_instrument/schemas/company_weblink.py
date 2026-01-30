"""CompanyWeblink Pydantic schemas."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class WeblinkTypeRef(BaseModel):
    """Weblink type reference."""
    weblink_type_id: int
    description: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CompanyRef(BaseModel):
    """Company reference for weblink with entity name."""
    company_id: int
    entity: EntityRef | None = None
    model_config = ConfigDict(from_attributes=True)


class CompanyWeblinkResponse(BaseModel):
    """Company weblink response."""
    company_weblink_id: int
    company_id: int
    url: str
    weblink_type_id: int
    last_updated: date
    company: CompanyRef | None = None
    weblink_type: WeblinkTypeRef | None = None
    model_config = ConfigDict(from_attributes=True)
