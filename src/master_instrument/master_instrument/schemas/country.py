"""Country Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class CountryResponse(BaseModel):
    """Country response schema."""

    country_id: int
    code_alpha2: str
    code_alpha3: str
    name: str

    model_config = ConfigDict(from_attributes=True)
