"""Dependencies for service injection."""

from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from master_instrument.db.session import get_db

# Core services
from master_instrument.services.instrument import InstrumentService
from master_instrument.services.venue import VenueService
from master_instrument.services.country import CountryService
from master_instrument.services.currency import CurrencyService
from master_instrument.services.entity import EntityService
from master_instrument.services.quote import QuoteService
from master_instrument.services.company import CompanyService
from master_instrument.services.equity import EquityService
from master_instrument.services.company_weblink import CompanyWeblinkService
from master_instrument.services.country_region import CountryRegionService

# Timeseries/data services
from master_instrument.services.market_data import MarketDataService
from master_instrument.services.adjusted_market_data import AdjustedMarketDataService
from master_instrument.services.company_market_cap import CompanyMarketCapService
from master_instrument.services.std_financial_value import StdFinancialValueService
from master_instrument.services.std_financial_item import StdFinancialItemService
from master_instrument.services.std_financial_statement import StdFinancialStatementService

# Type services
from master_instrument.services.corpact_type import CorpactTypeService
from master_instrument.services.dividend_type import DividendTypeService
from master_instrument.services.entity_type import EntityTypeService
from master_instrument.services.equity_type import EquityTypeService
from master_instrument.services.instrument_type import InstrumentTypeService
from master_instrument.services.region import RegionService
from master_instrument.services.venue_type import VenueTypeService
from master_instrument.services.weblink_type import WeblinkTypeService

# Mapping services
from master_instrument.services.mapping import VenueMappingService, QuoteMappingService

# Classification services
from master_instrument.services.classification import (
    ClassificationSchemeService,
    ClassificationNodeService,
    EntityClassificationService,
)


# ============== CORE SERVICES ==============

def get_instrument_service(db: Annotated[Session, Depends(get_db)]) -> InstrumentService:
    return InstrumentService(db)


def get_venue_service(db: Annotated[Session, Depends(get_db)]) -> VenueService:
    return VenueService(db)


def get_country_service(db: Annotated[Session, Depends(get_db)]) -> CountryService:
    return CountryService(db)


def get_currency_service(db: Annotated[Session, Depends(get_db)]) -> CurrencyService:
    return CurrencyService(db)


def get_entity_service(db: Annotated[Session, Depends(get_db)]) -> EntityService:
    return EntityService(db)


def get_quote_service(db: Annotated[Session, Depends(get_db)]) -> QuoteService:
    return QuoteService(db)


def get_company_service(db: Annotated[Session, Depends(get_db)]) -> CompanyService:
    return CompanyService(db)


def get_equity_service(db: Annotated[Session, Depends(get_db)]) -> EquityService:
    return EquityService(db)


def get_company_weblink_service(db: Annotated[Session, Depends(get_db)]) -> CompanyWeblinkService:
    return CompanyWeblinkService(db)


def get_country_region_service(db: Annotated[Session, Depends(get_db)]) -> CountryRegionService:
    return CountryRegionService(db)


# ============== TIMESERIES/DATA SERVICES ==============

def get_market_data_service(db: Annotated[Session, Depends(get_db)]) -> MarketDataService:
    return MarketDataService(db)


def get_adjusted_market_data_service(db: Annotated[Session, Depends(get_db)]) -> AdjustedMarketDataService:
    return AdjustedMarketDataService(db)


def get_company_market_cap_service(db: Annotated[Session, Depends(get_db)]) -> CompanyMarketCapService:
    return CompanyMarketCapService(db)


def get_std_financial_value_service(db: Annotated[Session, Depends(get_db)]) -> StdFinancialValueService:
    return StdFinancialValueService(db)


def get_std_financial_item_service(db: Annotated[Session, Depends(get_db)]) -> StdFinancialItemService:
    return StdFinancialItemService(db)


def get_std_financial_statement_service(db: Annotated[Session, Depends(get_db)]) -> StdFinancialStatementService:
    return StdFinancialStatementService(db)


# ============== TYPE SERVICES ==============

def get_corpact_type_service(db: Annotated[Session, Depends(get_db)]) -> CorpactTypeService:
    return CorpactTypeService(db)


def get_dividend_type_service(db: Annotated[Session, Depends(get_db)]) -> DividendTypeService:
    return DividendTypeService(db)


def get_entity_type_service(db: Annotated[Session, Depends(get_db)]) -> EntityTypeService:
    return EntityTypeService(db)


def get_equity_type_service(db: Annotated[Session, Depends(get_db)]) -> EquityTypeService:
    return EquityTypeService(db)


def get_instrument_type_service(db: Annotated[Session, Depends(get_db)]) -> InstrumentTypeService:
    return InstrumentTypeService(db)


def get_region_service(db: Annotated[Session, Depends(get_db)]) -> RegionService:
    return RegionService(db)


def get_venue_type_service(db: Annotated[Session, Depends(get_db)]) -> VenueTypeService:
    return VenueTypeService(db)


def get_weblink_type_service(db: Annotated[Session, Depends(get_db)]) -> WeblinkTypeService:
    return WeblinkTypeService(db)


# ============== MAPPING SERVICES ==============

def get_venue_mapping_service(db: Annotated[Session, Depends(get_db)]) -> VenueMappingService:
    return VenueMappingService(db)


def get_quote_mapping_service(db: Annotated[Session, Depends(get_db)]) -> QuoteMappingService:
    return QuoteMappingService(db)


# ============== CLASSIFICATION SERVICES ==============

def get_classification_scheme_service(db: Annotated[Session, Depends(get_db)]) -> ClassificationSchemeService:
    return ClassificationSchemeService(db)


def get_classification_node_service(db: Annotated[Session, Depends(get_db)]) -> ClassificationNodeService:
    return ClassificationNodeService(db)


def get_entity_classification_service(db: Annotated[Session, Depends(get_db)]) -> EntityClassificationService:
    return EntityClassificationService(db)
