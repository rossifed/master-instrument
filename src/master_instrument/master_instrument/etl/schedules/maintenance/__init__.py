"""Maintenance schedules - Database maintenance schedules."""

from typing import Any

# TimescaleDB compression is handled natively by TimescaleDB policies
# configured in alembic migration 0006
# No Dagster schedule needed for compression.

maintenance_schedules: list[Any] = []

__all__ = [
    "maintenance_schedules",
]
