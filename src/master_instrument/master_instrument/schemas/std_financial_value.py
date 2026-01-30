"""StdFinancialValue Pydantic schemas."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class FinancialPeriodTypeRef(BaseModel):
    """Financial period type reference."""
    financial_period_type_id: int
    name: str
    mnemonic: str | None = None
    months: int | None = None
    model_config = ConfigDict(from_attributes=True)


class FinancialStatementTypeRef(BaseModel):
    """Financial statement type reference."""
    financial_statement_type_id: int
    name: str
    mnemonic: str | None = None
    model_config = ConfigDict(from_attributes=True)


class StdFinancialItemRef(BaseModel):
    """Financial item reference."""
    std_financial_item_id: int
    name: str
    is_currency: bool
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CompanyRef(BaseModel):
    """Company reference."""
    company_id: int
    entity: EntityRef | None = None
    model_config = ConfigDict(from_attributes=True)


class StdFinancialValueResponse(BaseModel):
    """StdFinancialValue response schema."""
    std_financial_statement_id: int
    std_financial_item_id: int
    
    # Denormalized natural key
    company_id: int
    period_type_id: int
    period_end_date: date
    filing_end_date: date
    statement_type_id: int
    calendar_end_date: date
    
    # Value
    value: Decimal | None = None
    
    # Audit
    loaded_at: datetime
    
    # Relations
    company: CompanyRef | None = None
    std_financial_item: StdFinancialItemRef | None = None
    period_type: FinancialPeriodTypeRef | None = None
    statement_type: FinancialStatementTypeRef | None = None
    
    model_config = ConfigDict(from_attributes=True)
