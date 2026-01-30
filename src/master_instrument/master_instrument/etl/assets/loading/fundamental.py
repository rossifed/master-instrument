"""Assets for fundamental financial data loading."""

import os
from typing import Any

import dagster as dg
from dagster import asset, Failure, AssetKey

from master_instrument.etl.assets.loading.reference import countries, currencies, companies
from master_instrument.etl.assets.loading.config import FullLoadConfig, FULL_LOAD_CONFIRMATION_TOKEN
from master_instrument.etl import loading as ld
from master_instrument import models as m
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource

# Batch date range from environment (default: 2000-01-01)
TIMESERIES_START_DATE = os.getenv("TIMESERIES_START_DATE", "2000-01-01")


@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental"
)
def financial_period_type(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load financial period types from seed."""
    config = ld.MergeConfig.from_model(
        m.FinancialPeriodType,
        source_table="seed.financial_period_type",
        unique_key="financial_period_type_id",
        order_by="months"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental"
)
def financial_statement_type(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load financial statement types from seed."""
    config = ld.MergeConfig.from_model(
        m.FinancialStatementType,
        source_table="seed.financial_statement_type",
        unique_key="financial_statement_type_id",
        order_by="financial_statement_type_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental",
    deps=[
        companies,
        countries,
        currencies,
        financial_period_type,
        AssetKey(["intermediate", "fundamental", "int_std_financial_filing"])
    ]
)
def std_financial_filing(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load standardized financial filings."""
    config = ld.MergeConfig.from_model(
        m.StdFinancialFiling,
        source_table="intermediate.int_std_financial_filing",
        unique_key=["company_id", "period_end_date", "filing_end_date", "period_type_id"]
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental",
    deps=[
        std_financial_filing,
        financial_statement_type,
        AssetKey(["intermediate", "fundamental", "int_std_financial_statement"])
    ]
)
def std_financial_statement(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load standardized financial statements."""
    config = ld.UpsertConfig.from_model(
        m.StdFinancialStatement,
        source_table="intermediate.int_std_financial_statement",
        unique_key=["std_financial_filing_id", "statement_type_id"]
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental",
    deps=[
        financial_statement_type,
        AssetKey(["intermediate", "fundamental", "int_std_financial_item"])
    ]
)
def std_financial_item(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load standardized financial items."""
    config = ld.UpsertConfig.from_model(
        m.StdFinancialItem,
        source_table="intermediate.int_std_financial_item",
        unique_key="std_financial_item_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master","full", "fundamental"],
    group_name="fundamental_full_load",
    deps=[
        std_financial_statement,
        std_financial_item,
        AssetKey(["intermediate", "fundamental", "int_std_financial_value_full"])
    ],
    tags={
        "dagster/priority": "-1",
        "dangerous": "true",
        "load_type": "full",
        "estimated_duration": "4h",
        "estimated_rows": "1B",
    },
    owners=["team:data-engineering"],
)
def std_financial_value_full(
    context: dg.AssetExecutionContext,
    config: FullLoadConfig,
    engine: SqlAlchemyEngineResource
):
    """⚠️ DANGEROUS: Full load of financial values (~1B+ rows, ~4 hours).

    This asset TRUNCATES the std_financial_value table and reloads everything.
    Only run during initial setup or disaster recovery.

    Requires config: confirm_full_load = "YES"
    """
    # Safety check: require explicit confirmation
    if config.confirm_full_load != FULL_LOAD_CONFIRMATION_TOKEN:
        raise Failure(
            description=(
                f"⛔ FULL LOAD BLOCKED: This asset will TRUNCATE and reload ~1B+ rows.\n"
                f"If you really want to run this, provide config:\n"
                f'  confirm_full_load: "{FULL_LOAD_CONFIRMATION_TOKEN}"'
            )
        )

    context.log.warning("🚨 FULL LOAD CONFIRMED - Starting std_financial_value full reload...")

    strategy = ld.FixedIntervalStrategy(
        interval=30,
        unit=ld.IntervalUnit.DAY,
        min_date=TIMESERIES_START_DATE
    )

    config_ld = ld.InsertConfig.from_model(
        m.StdFinancialValue,
        source_table="intermediate.int_std_financial_value_full",
        unique_key=["std_financial_statement_id", "std_financial_item_id"],
        batch=ld.BatchConfig(
            batch_date_column="period_end_date",
            truncate_before_load=True,
            drop_indexes=True,
            drop_fk=True,      # Drop Foreign Keys during load (5 FK = ~77% overhead)
            drop_unique=True,  # Drop UNIQUE constraints
            fail_fast=True,
            disable_wal=True,
            disable_autovacuum=True
        )
    )

    loader = ld.BatchLoader(engine.get_engine(), ld.TemplateSource(config_ld), strategy, logger=context.log)
    return loader.load()

@asset(
    key_prefix=["master", "fundamental"],
    group_name="fundamental",
    deps=[
        std_financial_statement,
        std_financial_item,
        AssetKey(["intermediate", "fundamental", "int_std_financial_value_changes"])
    ]
)
def std_financial_values_changes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load standardized financial values from change tracking (CDC)."""
    
    config = ld.CDCConfig.from_model(
        m.StdFinancialValue,
        source_table="intermediate.int_std_financial_value_changes",
        unique_key=["std_financial_statement_id", "std_financial_item_id"],
        tracking_table="master.std_financial_value_load"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
