"""ETL schedules - Data loading schedules."""

from master_instrument.etl.schedules.etl.daily import (
    daily_schedules,
    cdc_schedules,
    reference_daily_schedule,
    market_daily_schedule,
    fundamental_daily_schedule,
    market_cdc_schedule,
    fundamental_cdc_schedule,
)

etl_schedules = daily_schedules + cdc_schedules

__all__ = [
    "etl_schedules",
    "daily_schedules",
    "cdc_schedules",
    "reference_daily_schedule",
    "market_daily_schedule",
    "fundamental_daily_schedule",
    "market_cdc_schedule",
    "fundamental_cdc_schedule",
]
