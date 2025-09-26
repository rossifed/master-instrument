# src/master_instrument/definitions.py

from dagster import load_asset_checks_from_modules, load_assets_from_modules, Definitions
from master_instrument.assets import sling
from master_instrument.resources.sling.sling_resources import sling_resources

all_assets = load_assets_from_modules([
    sling
])
all_asset_checks = load_asset_checks_from_modules([
    sling
])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources={
        "sling": sling_resources,
    },
)
