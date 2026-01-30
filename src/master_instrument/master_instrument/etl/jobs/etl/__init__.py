"""ETL jobs - Data loading and transformation."""

from master_instrument.etl.jobs.etl.daily import (
    daily_jobs,
    cdc_jobs,
    reference_daily_job,
    market_daily_job,
    fundamental_daily_job,
    market_cdc_job,
    fundamental_cdc_job,
)

etl_jobs = daily_jobs + cdc_jobs

__all__ = [
    "etl_jobs",
    "daily_jobs",
    "cdc_jobs",
    "reference_daily_job",
    "market_daily_job",
    "fundamental_daily_job",
    "market_cdc_job",
    "fundamental_cdc_job",
]
