"""StdFinancialStatement Pydantic schemas."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class FinancialStatementTypeRef(BaseModel):
    """Financial statement type reference."""
    financial_statement_type_id: int
    name: str
    mnemonic: str | None = None
    model_config = ConfigDict(from_attributes=True)


class StdFinancialFilingRef(BaseModel):
    """Filing reference for statements."""
    std_financial_filing_id: int
    company_id: int
    period_end_date: date
    filing_end_date: date
    model_config = ConfigDict(from_attributes=True)


class StdFinancialStatementResponse(BaseModel):
    """StdFinancialStatement response schema."""
    std_financial_statement_id: int
    std_financial_filing_id: int
    statement_type_id: int
    
    is_complete: bool
    is_consolidated: bool
    
    public_date: date | None = None
    last_update_date: date | None = None
    source: str | None = None
    
    # Relations
    statement_type: FinancialStatementTypeRef | None = None
    std_financial_filing: StdFinancialFilingRef | None = None
    
    model_config = ConfigDict(from_attributes=True)
