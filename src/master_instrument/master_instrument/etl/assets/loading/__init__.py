"""Data loading assets - loading raw data from external sources.

This package contains Dagster assets for ingesting data into the warehouse:
- reference: Static reference data (countries, currencies, companies, etc.)
- market: Market and pricing data (quotes, market data, adjustments)
- fundamental: Company fundamental data (financial statements, etc.)

Legacy assets are in *_ZZZ.py files (deprecated, kept for reference).
"""
from master_instrument.etl.assets.loading.reference import *  # noqa: F401, F403
from master_instrument.etl.assets.loading.market import *  # noqa: F401, F403
from master_instrument.etl.assets.loading.fundamental import *  # noqa: F401, F403
