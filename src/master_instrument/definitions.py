# src/master_instrument/definitions.py

from dagster import load_asset_checks_from_modules, load_assets_from_modules, Definitions
from master_instrument.assets import sling, dbt, load
from master_instrument.resources.sling.sling_resources import sling_resources
from master_instrument.resources.dbt_resources import dbt_resource
from master_instrument.resources.sqlalchemy_resource import SqlAlchemyEngineResource
all_assets = load_assets_from_modules([
    sling, dbt, load
])
all_asset_checks = load_asset_checks_from_modules([
    sling, dbt, load
])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources={
        "sling": sling_resources,
        "dbt": dbt_resource,
        "engine": SqlAlchemyEngineResource(
            connection_string="postgresql://postgres:postgres123@host.docker.internal:5434/master_instrument?sslmode=disable"
        ),
    },
)
