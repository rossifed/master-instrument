"""Company Pydantic schemas."""

from datetime import date
from pydantic import BaseModel, ConfigDict


class EntityTypeRef(BaseModel):
    """Entity type reference."""
    entity_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference for Company."""
    entity_id: int
    name: str
    description: str | None = None
    entity_type: EntityTypeRef | None = None
    model_config = ConfigDict(from_attributes=True)


class CountryRef(BaseModel):
    """Country reference."""
    country_id: int
    code_alpha2: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class CurrencyRef(BaseModel):
    """Currency reference."""
    currency_id: int
    code: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class CompanyResponse(BaseModel):
    """Company response schema - based on Company model with ORM relations."""
    
    company_id: int
    
    # Company fields
    employee_count: int | None = None
    employee_count_date: date | None = None
    st_address_1: str | None = None
    st_address_2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    phone: str | None = None
    industry_code: str | None = None
    public_since: date | None = None
    common_shareholders: int | None = None
    common_shareholders_date: date | None = None
    total_shares_outstanding: int | None = None
    shares_outstanding_updated_at: date | None = None
    total_float_shares: int | None = None
    ultimate_organization_id: int | None = None
    primary_company_id: int | None = None
    last_modified_financial_at: date | None = None
    last_modified_non_financial_at: date | None = None
    latest_annual_financial_date: date | None = None
    latest_interim_financial_date: date | None = None
    organization_id: int | None = None
    
    # Relations (loaded via lazy="selectin")
    entity: EntityRef | None = None
    country: CountryRef | None = None
    estimates_currency: CurrencyRef | None = None
    statements_currency: CurrencyRef | None = None

    model_config = ConfigDict(from_attributes=True)
