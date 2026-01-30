"""All Dagster schedules aggregated here."""

from master_instrument.etl.schedules.etl import etl_schedules
from master_instrument.etl.schedules.maintenance import maintenance_schedules

all_schedules = etl_schedules + maintenance_schedules

__all__ = ["all_schedules", "etl_schedules", "maintenance_schedules"]
