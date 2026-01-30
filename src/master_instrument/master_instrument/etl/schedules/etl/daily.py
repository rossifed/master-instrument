"""Simplified daily schedules.

Architecture:
- One schedule per domain (staggered to avoid resource contention)
- No sensors needed - Dagster handles asset dependencies automatically
- CDC schedules run during the day for incremental updates
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from master_instrument.etl.jobs.etl.daily import (
    reference_daily_job,
    market_daily_job,
    fundamental_daily_job,
    market_cdc_job,
    fundamental_cdc_job,
)

# Timezone for all schedules
TIMEZONE = "CET"

# =============================================================================
# MORNING SCHEDULES (Full pipeline per domain)
# Staggered to avoid resource contention
# =============================================================================

reference_daily_schedule = ScheduleDefinition(
    job=reference_daily_job,
    cron_schedule="0 6 * * 1-5",  # 06:00 CET, Mon-Fri
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)

market_daily_schedule = ScheduleDefinition(
    job=market_daily_job,
    cron_schedule="15 6 * * 1-5",  # 06:15 CET, Mon-Fri
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)

fundamental_daily_schedule = ScheduleDefinition(
    job=fundamental_daily_job,
    cron_schedule="30 6 * * 1-5",  # 06:30 CET, Mon-Fri
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)

# =============================================================================
# CDC SCHEDULES (Intra-day incremental updates)
# =============================================================================

market_cdc_schedule = ScheduleDefinition(
    job=market_cdc_job,
    cron_schedule="0 */2 * * 1-5",  # Every 2 hours, Mon-Fri
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)

fundamental_cdc_schedule = ScheduleDefinition(
    job=fundamental_cdc_job,
    cron_schedule="30 */2 * * 1-5",  # Every 2 hours (offset 30min), Mon-Fri
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)

# =============================================================================
# Export all ETL schedules
# =============================================================================

daily_schedules = [
    reference_daily_schedule,
    market_daily_schedule,
    fundamental_daily_schedule,
]

cdc_schedules = [
    market_cdc_schedule,
    fundamental_cdc_schedule,
]
