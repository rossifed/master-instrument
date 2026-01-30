"""CountryRegion Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class CountryRef(BaseModel):
    """Country reference."""
    country_id: int
    name: str
    alpha2: str | None = None
    model_config = ConfigDict(from_attributes=True)


class RegionRef(BaseModel):
    """Region reference."""
    region_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CountryRegionResponse(BaseModel):
    """Country-Region association response."""
    country_id: int
    region_id: int
    country: CountryRef | None = None
    region: RegionRef | None = None
    model_config = ConfigDict(from_attributes=True)
