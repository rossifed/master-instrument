"""EquityType Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class EquityTypeResponse(BaseModel):
    """EquityType response schema."""
    equity_type_id: int
    mnemonic: str
    description: str
    model_config = ConfigDict(from_attributes=True)
