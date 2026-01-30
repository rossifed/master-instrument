"""Maintenance jobs - Database maintenance, compression, cleanup."""

from typing import Any

# TimescaleDB compression is handled natively by TimescaleDB policies
# configured in alembic migration 0006
# No Dagster job needed for compression.

maintenance_jobs: list[Any] = []

__all__ = [
    "maintenance_jobs",
]
