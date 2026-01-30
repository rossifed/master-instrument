"""Currency Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class CurrencyResponse(BaseModel):
    """Currency response schema."""

    currency_id: int
    code: str
    name: str

    model_config = ConfigDict(from_attributes=True)
