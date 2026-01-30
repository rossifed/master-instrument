"""Equity Pydantic schemas."""

from datetime import date
from pydantic import BaseModel, ConfigDict


class InstrumentTypeRef(BaseModel):
    """Instrument type reference."""
    instrument_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    description: str | None = None
    model_config = ConfigDict(from_attributes=True)


class InstrumentRef(BaseModel):
    """Instrument reference for Equity."""
    instrument_id: int
    name: str
    symbol: str | None = None
    description: str | None = None
    instrument_type: InstrumentTypeRef | None = None
    entity: EntityRef | None = None
    model_config = ConfigDict(from_attributes=True)


class EquityTypeRef(BaseModel):
    """Equity type reference."""
    equity_type_id: int
    mnemonic: str
    description: str | None = None
    model_config = ConfigDict(from_attributes=True)


class CountryRef(BaseModel):
    """Country reference."""
    country_id: int
    code_alpha2: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EquityResponse(BaseModel):
    """Equity response schema - based on Equity model with ORM relations."""
    
    equity_id: int
    security_id: int
    
    # Identifiers
    isin: str | None = None
    cusip: str | None = None
    sedol: str | None = None
    ric: str | None = None
    ticker: str | None = None
    
    # Equity details
    delisted_date: date | None = None
    issue_type: str | None = None
    issue_description: str | None = None
    div_unit: str | None = None
    is_major_security: bool
    split_date: date | None = None
    split_factor: float | None = None
    
    # Relations (loaded via lazy="selectin")
    instrument: InstrumentRef | None = None
    equity_type: EquityTypeRef | None = None
    country: CountryRef | None = None

    model_config = ConfigDict(from_attributes=True)
