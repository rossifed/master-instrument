"""Shared configuration for loading assets."""

from dagster import Config

# Confirmation token required for dangerous full load operations
FULL_LOAD_CONFIRMATION_TOKEN = "YES"


class FullLoadConfig(Config):
    """Configuration for dangerous full load assets.

    Requires explicit confirmation to prevent accidental execution.
    """
    confirm_full_load: str = ""
