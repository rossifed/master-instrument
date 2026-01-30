"""CompanyMarketCap Pydantic schemas."""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class CurrencyRef(BaseModel):
    """Currency reference."""
    currency_id: int
    code: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CompanyRef(BaseModel):
    """Company reference for market cap."""
    company_id: int
    entity: EntityRef | None = None
    model_config = ConfigDict(from_attributes=True)


class CompanyMarketCapResponse(BaseModel):
    """CompanyMarketCap response schema."""
    valuation_date: date
    company_id: int
    currency_id: int
    
    market_cap: float | None = None
    shares_outstanding: int | None = None
    
    # Audit
    loaded_at: datetime
    
    # Relations
    company: CompanyRef | None = None
    currency: CurrencyRef | None = None
    
    model_config = ConfigDict(from_attributes=True)
