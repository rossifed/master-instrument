"""DividendType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class DividendTypeResponse(BaseModel):
    """DividendType response schema."""
    dividend_type_id: int
    code: str
    description: str
    model_config = ConfigDict(from_attributes=True)
