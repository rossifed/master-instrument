"""Mapping Pydantic schemas - simplified to venue and quote mappings only."""

from pydantic import BaseModel, ConfigDict


class DataSourceRef(BaseModel):
    """DataSource reference."""
    data_source_id: int
    mnemonic: str
    description: str
    model_config = ConfigDict(from_attributes=True)


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


class InstrumentRef(BaseModel):
    """Instrument reference."""
    instrument_id: int
    name: str
    symbol: str | None = None
    model_config = ConfigDict(from_attributes=True)


class QuoteRef(BaseModel):
    """Quote reference for mapping."""
    quote_id: int
    instrument_id: int
    venue_id: int
    is_primary: bool
    ticker: str | None = None
    ric: str | None = None
    mic: str | None = None
    instrument: InstrumentRef | None = None
    venue: VenueRef | None = None
    model_config = ConfigDict(from_attributes=True)


class VenueMappingResponse(BaseModel):
    """VenueMapping response."""
    internal_venue_id: int
    external_venue_id: str
    data_source: DataSourceRef
    venue: VenueRef | None = None
    model_config = ConfigDict(from_attributes=True)


class QuoteMappingResponse(BaseModel):
    """QuoteMapping response."""
    internal_quote_id: int
    external_quote_id: str
    data_source: DataSourceRef
    quote: QuoteRef | None = None
    model_config = ConfigDict(from_attributes=True)
