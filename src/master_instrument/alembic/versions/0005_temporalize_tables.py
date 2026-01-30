"""Temporalize selected reference tables

Revision ID: 0005_temporalize_tables
Revises: 0004_create_tables
"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import Union
from utils.temporal import temporalize_tables, detemporalize_tables


revision: str = '0005_temporalize_tables'
down_revision: Union[str, None] = '0004_create_tables'
branch_labels = None
depends_on = None

SCHEMA = "master"

TABLES = [
    # Core reference tables
    "classification_level",
    "classification_node",
    "classification_scheme",
    "company",
    "entity_mapping",
    "company_weblink",
    "country",
    "country_region",
    "currency",
    "currency_pair",
    "data_source",
    "entity",
    "entity_classification",
    "entity_type",
    "equity",
    "equity_type",
    "instrument",
    "instrument_mapping",
    "instrument_type",
    "quote",
    "quote_mapping",
    "region",
    "venue",
    "venue_mapping",
    "venue_type",
    "weblink_type",
    # Corporate actions
    "corpact_event",
    "corpact_type",
    "corpact_adjustment",
    "dividend",
    "dividend_type",
    "dividend_adjustment",
    # Financial reporting metadata
    "financial_period_type",
    "financial_statement_type",
    "std_financial_statement",
    "std_financial_item",
    "std_financial_item_mapping",
    "std_financial_filing",
    # NOT temporalized (time-series / hypertables / tracking):
    # - market_data, market_data_load
    # - company_market_cap, company_market_cap_load  
    # - fx_rate, share_outstanding
    # - std_financial_value
]

def upgrade():
    temporalize_tables(SCHEMA, TABLES)

def downgrade():
    detemporalize_tables(SCHEMA, TABLES)
