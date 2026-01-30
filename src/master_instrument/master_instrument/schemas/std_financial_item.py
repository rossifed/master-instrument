"""StdFinancialItem Pydantic schemas."""

from pydantic import BaseModel, ConfigDict


class StdFinancialItemResponse(BaseModel):
    """StdFinancialItem response schema."""
    std_financial_item_id: int
    name: str
    statement_type_id: int | None = None
    is_currency: bool
    
    model_config = ConfigDict(from_attributes=True)
