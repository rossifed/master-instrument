"""Quote Pydantic schemas."""

from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class VenueTypeRef(BaseModel):
    """VenueType reference."""
    venue_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class VenueRef(BaseModel):
    """Venue reference."""
    venue_id: int
    name: str
    mnemonic: str
    venue_type: VenueTypeRef | None = None
    model_config = ConfigDict(from_attributes=True)


class InstrumentTypeRef(BaseModel):
    """InstrumentType reference."""
    instrument_type_id: int
    mnemonic: str
    name: str
    model_config = ConfigDict(from_attributes=True)


class EntityRef(BaseModel):
    """Entity reference."""
    entity_id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class InstrumentRef(BaseModel):
    """Instrument reference."""
    instrument_id: int
    name: str
    symbol: str | None = None
    instrument_type: InstrumentTypeRef | None = None
    entity: EntityRef | None = None
    model_config = ConfigDict(from_attributes=True)


class QuoteResponse(BaseModel):
    """Quote response schema."""
    quote_id: int
    instrument_id: int
    venue_id: int
    currency_id: int | None = None
    is_primary: bool
    ticker: str | None = None
    ric: str | None = None
    mic: str | None = None
    market_name: str | None = None
    price_unit: Decimal | None = None
    
    # Relations
    instrument: InstrumentRef | None = None
    venue: VenueRef | None = None
    
    model_config = ConfigDict(from_attributes=True)
