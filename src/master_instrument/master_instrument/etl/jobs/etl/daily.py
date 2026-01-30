"""Simplified daily jobs - one job per domain.

Architecture:
- One job per domain that runs the full pipeline (raw → staging → intermediate → master)
- Dagster respects asset dependencies automatically - no sensors needed
- Full load assets are always excluded (matched by *_full suffix)
- CDC jobs for intra-day updates (market and fundamental only)
"""

from dagster import define_asset_job, AssetSelection

# =============================================================================
# FULL LOAD EXCLUSIONS
# These assets are NEVER run in daily jobs (manual only)
# Excludes all assets with key prefix master/full/* or raw/full/*
# =============================================================================

full_load_selection = (
    AssetSelection.key_prefixes(["master", "full"]) |
    AssetSelection.key_prefixes(["raw", "full"])
)

# =============================================================================
# DOMAIN SELECTIONS (all layers combined)
# =============================================================================

# Reference: all layers
reference_all_selection = (
    AssetSelection.key_prefixes(["raw", "qa", "reference"]) |
    AssetSelection.key_prefixes(["staging", "qa", "reference"]) |
    AssetSelection.key_prefixes(["staging", "s3", "reference"]) |
    AssetSelection.key_prefixes(["intermediate", "reference"]) |
    AssetSelection.key_prefixes(["master", "reference"])
) - full_load_selection

# Market: all layers
market_all_selection = (
    AssetSelection.key_prefixes(["raw", "qa", "market"]) |
    AssetSelection.key_prefixes(["staging", "qa", "market"]) |
    AssetSelection.key_prefixes(["intermediate", "market"]) |
    AssetSelection.key_prefixes(["master", "market"])
) - full_load_selection

# Fundamental: all layers
fundamental_all_selection = (
    AssetSelection.key_prefixes(["raw", "qa", "fundamental"]) |
    AssetSelection.key_prefixes(["staging", "qa", "fundamental"]) |
    AssetSelection.key_prefixes(["intermediate", "fundamental"]) |
    AssetSelection.key_prefixes(["master", "fundamental"])
) - full_load_selection

# =============================================================================
# DAILY JOBS (morning full refresh per domain)
# =============================================================================

reference_daily_job = define_asset_job(
    name="etl__reference_daily",
    description="Daily reference data pipeline (raw → staging → intermediate → master)",
    selection=reference_all_selection,
    tags={"category": "etl", "domain": "reference", "schedule": "daily"},
)

market_daily_job = define_asset_job(
    name="etl__market_daily",
    description="Daily market data pipeline (raw → staging → intermediate → master)",
    selection=market_all_selection,
    tags={"category": "etl", "domain": "market", "schedule": "daily"},
)

fundamental_daily_job = define_asset_job(
    name="etl__fundamental_daily",
    description="Daily fundamental data pipeline (raw → staging → intermediate → master)",
    selection=fundamental_all_selection,
    tags={"category": "etl", "domain": "fundamental", "schedule": "daily"},
)

# =============================================================================
# CDC JOBS (intra-day incremental updates)
# Only market and fundamental have CDC assets
# =============================================================================

# Market CDC: only the *_changes assets in master layer
market_cdc_selection = AssetSelection.assets(
    ["master", "market", "market_data_changes"],
    ["master", "market", "company_market_cap_changes"],
    ["master", "market", "total_return_changes"],
)

market_cdc_job = define_asset_job(
    name="etl__market_cdc",
    description="Intra-day market data CDC updates",
    selection=market_cdc_selection,
    tags={"category": "etl", "domain": "market", "schedule": "cdc"},
)

# Fundamental CDC: only the *_changes assets in master layer
fundamental_cdc_selection = AssetSelection.assets(
    ["master", "fundamental", "std_financial_values_changes"],
)

fundamental_cdc_job = define_asset_job(
    name="etl__fundamental_cdc",
    description="Intra-day fundamental data CDC updates",
    selection=fundamental_cdc_selection,
    tags={"category": "etl", "domain": "fundamental", "schedule": "cdc"},
)

# =============================================================================
# Export all ETL jobs
# =============================================================================

daily_jobs = [
    reference_daily_job,
    market_daily_job,
    fundamental_daily_job,
]

cdc_jobs = [
    market_cdc_job,
    fundamental_cdc_job,
]
