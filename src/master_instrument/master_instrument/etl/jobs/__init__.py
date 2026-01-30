"""All Dagster jobs aggregated here."""

from master_instrument.etl.jobs.etl import etl_jobs
from master_instrument.etl.jobs.maintenance import maintenance_jobs

all_jobs = etl_jobs + maintenance_jobs

__all__ = ["all_jobs", "etl_jobs", "maintenance_jobs"]
