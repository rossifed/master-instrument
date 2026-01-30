"""Assets for market data loading."""

import os
from typing import Any

import dagster as dg
from dagster import asset, Failure, AssetKey
from master_instrument.etl.assets.loading.reference import quotes, currencies, companies, equities, dividends
from master_instrument.etl.assets.loading.config import FullLoadConfig, FULL_LOAD_CONFIRMATION_TOKEN
from master_instrument.etl import loading as ld
from master_instrument import models as m
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource

# Batch date range from environment (default: 2000-01-01)
TIMESERIES_START_DATE = os.getenv("TIMESERIES_START_DATE", "2000-01-01")


@asset(
    key_prefix=["master","full", "market"],
    group_name="market_full_load",
    deps=[
        quotes,
        currencies,
        AssetKey(["intermediate", "market", "int_market_data_full"])
    ],
    tags={
        "dagster/priority": "-1",  # Low priority to avoid accidental scheduling
        "dangerous": "true",
        "load_type": "full",
        "estimated_duration": "2h",
        "estimated_rows": "500M",
    },
    owners=["team:data-engineering"],
)
def market_data_full(
    context: dg.AssetExecutionContext,
    config: FullLoadConfig,
    engine: SqlAlchemyEngineResource
):
    """⚠️ DANGEROUS: Full load of market data (~500M rows, ~2 hours).

    This asset TRUNCATES the market_data table and reloads everything.
    Only run during initial setup or disaster recovery.

    Requires config: confirm_full_load = "YES"
    """
    # Safety check: require explicit confirmation
    if config.confirm_full_load != FULL_LOAD_CONFIRMATION_TOKEN:
        raise Failure(
            description=(
                f"⛔ FULL LOAD BLOCKED: This asset will TRUNCATE and reload ~500M rows.\n"
                f"If you really want to run this, provide config:\n"
                f'  confirm_full_load: "{FULL_LOAD_CONFIRMATION_TOKEN}"'
            )
        )

    context.log.warning("🚨 FULL LOAD CONFIRMED - Starting market_data full reload...")

    strategy = ld.FixedIntervalStrategy(
        interval=90,
        unit=ld.IntervalUnit.DAY,
        min_date=TIMESERIES_START_DATE  # From env var - skips slow MIN scan
    )

    config_ld = ld.InsertConfig.from_model(
        m.MarketData,
        source_table="intermediate.int_market_data_full",
        unique_key=["trade_date", "quote_id"],
        batch=ld.BatchConfig(
            batch_date_column="trade_date",
            truncate_before_load=True,
            drop_indexes=True,
            fail_fast=True,
            is_hypertable=True,  # TimescaleDB: no CONCURRENTLY for index recreation
        )
    )

    loader = ld.BatchLoader(engine.get_engine(), ld.TemplateSource(config_ld), strategy, logger=context.log)
    return loader.load()

@asset(
    key_prefix=["master", "market"],
    group_name="market",
    deps=[
        currencies,
        AssetKey(["intermediate", "market", "int_fx_rate"])
    ]
)
def fx_rates(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load FX rates with hard delete for removed rates."""
    config = ld.UpsertConfig.from_model(
        m.FxRate,
        source_table="intermediate.int_fx_rate",
        unique_key=["rate_date", "base_currency_id", "quote_currency_id"],
        hard_delete=True,
        is_hypertable=True  # fx_rate is a compressed hypertable
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master", "market"],
    group_name="market",
    deps=[
        quotes,
        currencies,
        AssetKey(["intermediate", "market", "int_market_data_changes"])
    ]
)
def market_data_changes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load market data changes (CDC)."""
    config = ld.CDCConfig.from_model(
        m.MarketData,
        source_table="intermediate.int_market_data_changes",
        unique_key=["trade_date", "quote_id"],
        tracking_table="master.market_data_load",
        is_hypertable=True,
        hypertable_decompression_limit=2000000
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()
@asset(
    key_prefix=["master","full", "market"],
    group_name="market_full_load",
    deps=[
        companies,
        currencies,
        AssetKey(["intermediate", "market", "int_company_market_cap_full"])
    ],
    tags={
        "dagster/priority": "-1",
        "dangerous": "true",
        "load_type": "full",
        "estimated_duration": "1.5h",
        "estimated_rows": "500M",
    },
    owners=["team:data-engineering"],
)
def company_market_cap_full(
    context: dg.AssetExecutionContext,
    config: FullLoadConfig,
    engine: SqlAlchemyEngineResource
):
    """⚠️ DANGEROUS: Full load of company market cap (~500M rows, ~1.5 hours).

    This asset TRUNCATES the company_market_cap table and reloads everything.
    Only run during initial setup or disaster recovery.

    Requires config: confirm_full_load = "YES"
    """
    # Safety check: require explicit confirmation
    if config.confirm_full_load != FULL_LOAD_CONFIRMATION_TOKEN:
        raise Failure(
            description=(
                f"⛔ FULL LOAD BLOCKED: This asset will TRUNCATE and reload ~500M rows.\n"
                f"If you really want to run this, provide config:\n"
                f'  confirm_full_load: "{FULL_LOAD_CONFIRMATION_TOKEN}"'
            )
        )

    context.log.warning("🚨 FULL LOAD CONFIRMED - Starting company_market_cap full reload...")

    strategy = ld.FixedIntervalStrategy(
        interval=90,
        unit=ld.IntervalUnit.DAY,
        min_date=TIMESERIES_START_DATE
    )

    config_ld = ld.InsertConfig.from_model(
        m.CompanyMarketCap,
        source_table="intermediate.int_company_market_cap_full",
        unique_key=["valuation_date", "company_id"],
        batch=ld.BatchConfig(
            batch_date_column="valuation_date",
            truncate_before_load=True,
            drop_indexes=True,
            fail_fast=True,
            disable_wal=True,  # Convert to UNLOGGED during load
            disable_autovacuum=True,  # Bulk load optimization
        )
    )

    loader = ld.BatchLoader(engine.get_engine(), ld.TemplateSource(config_ld), strategy, logger=context.log)
    return loader.load()
@asset(
    key_prefix=["master", "market"],
    group_name="market",
    deps=[
        companies,
        currencies,
        AssetKey(["intermediate", "market", "int_company_market_cap_changes"])
    ]
)
def company_market_cap_changes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load company market cap changes (CDC)."""
    config = ld.CDCConfig.from_model(
        m.CompanyMarketCap,
        source_table="intermediate.int_company_market_cap_changes",
        unique_key=["valuation_date", "company_id"],
        tracking_table="master.company_market_cap_load",
        is_hypertable=True
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


# =============================================================================
# TOTAL RETURN ASSETS
# =============================================================================

@asset(
    key_prefix=["master", "full", "market"],
    group_name="market_full_load",
    deps=[
        quotes,
        AssetKey(["intermediate", "market", "int_total_return_full"])
    ],
    tags={
        "dagster/priority": "-1",  # Low priority to avoid accidental scheduling
        "dangerous": "true",
        "load_type": "full",
        "estimated_duration": "1.5h",
        "estimated_rows": "350M",
    },
    owners=["team:data-engineering"],
)
def total_return_full(
    context: dg.AssetExecutionContext,
    config: FullLoadConfig,
    engine: SqlAlchemyEngineResource
):
    """⚠️ DANGEROUS: Full load of total return index (~350M rows, ~1.5 hours).

    This asset TRUNCATES the total_return table and reloads everything.
    Only run during initial setup or disaster recovery.

    Optimizations applied:
    - Drops indexes/constraints before load
    - Disables WAL (UNLOGGED table during load)
    - Disables autovacuum during bulk insert

    Requires config: confirm_full_load = "YES"
    """
    # Safety check: require explicit confirmation
    if config.confirm_full_load != FULL_LOAD_CONFIRMATION_TOKEN:
        raise Failure(
            description=(
                f"⛔ FULL LOAD BLOCKED: This asset will TRUNCATE and reload ~350M rows.\n"
                f"If you really want to run this, provide config:\n"
                f'  confirm_full_load: "{FULL_LOAD_CONFIRMATION_TOKEN}"'
            )
        )

    context.log.warning("🚨 FULL LOAD CONFIRMED - Starting total_return full reload...")

    strategy = ld.FixedIntervalStrategy(
        interval=90,
        unit=ld.IntervalUnit.DAY,
        min_date=TIMESERIES_START_DATE  # From env var - skips slow MIN scan
    )

    config_ld = ld.InsertConfig.from_model(
        m.TotalReturn,
        source_table="intermediate.int_total_return_full",
        unique_key=["value_date", "quote_id"],
        batch=ld.BatchConfig(
            batch_date_column="value_date",
            truncate_before_load=True,
            drop_indexes=True,
            drop_fk=True,  # Drop Foreign Keys during load
            drop_pk=True,  # Drop Primary Key during load
            drop_unique=True,  # Drop UNIQUE constraints
            fail_fast=True,
            is_hypertable=True,  # TimescaleDB: no CONCURRENTLY for index recreation
            disable_wal=True,  # Convert to UNLOGGED during load (much faster)
            disable_autovacuum=True,  # Bulk load optimization
        )
    )

    loader = ld.BatchLoader(engine.get_engine(), ld.TemplateSource(config_ld), strategy, logger=context.log)
    return loader.load()


@asset(
    key_prefix=["master", "market"],
    group_name="market",
    deps=[
        quotes,
        AssetKey(["intermediate", "market", "int_total_return_changes"])
    ]
)
def total_return_changes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load total return index changes (CDC)."""
    config = ld.CDCConfig.from_model(
        m.TotalReturn,
        source_table="intermediate.int_total_return_changes",
        unique_key=["value_date", "quote_id"],
        tracking_table="master.total_return_load",
        is_hypertable=True,
        hypertable_decompression_limit=2000000
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()



@asset(
    key_prefix=["master", "full","reference"],
    group_name="market_full_load", #placed here as a full job because of performances issues, to don't impact reference loads
    deps=[
        equities,
        dividends,
        currencies,
        AssetKey(["intermediate", "reference", "int_dividend_adjustment"])
    ]
)
def dividend_adjustments(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load dividend adjustments using from_model() with DividendAdjustment model.
    
    Calculates cumulative dividend adjustment factors for price adjustment.
    Depends on dividends and market_data (via the int_dividend_adjustment view).
    """
    config = ld.MergeConfig.from_model(
        m.DividendAdjustment,
        source_table="intermediate.int_dividend_adjustment",
        unique_key=["equity_id", "ex_div_date"],
        order_by="equity_id, ex_div_date"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()