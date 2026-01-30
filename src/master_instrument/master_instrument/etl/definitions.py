# src/master_instrument/definitions.py

from dagster import load_asset_checks_from_modules, load_assets_from_modules, Definitions, EnvVar
from master_instrument.etl.assets import sling, dbt, infrastructure
from master_instrument.etl.assets import loading as asset_loading
from master_instrument.etl.resources.sling.sling_resources import sling_resources
from master_instrument.etl.resources.dbt_resources import dbt_resource
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource

# Jobs and schedules (aggregated in their respective __init__.py)
from master_instrument.etl.jobs import all_jobs
from master_instrument.etl.schedules import all_schedules

all_assets = load_assets_from_modules([
    sling, dbt, asset_loading, infrastructure
])
all_asset_checks = load_asset_checks_from_modules([
    sling, dbt, asset_loading
])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "sling": sling_resources,
        "dbt": dbt_resource,
        "engine": SqlAlchemyEngineResource(
            connection_string=EnvVar("REFERENTIAL_POSTGRES_CONN")
        )
    },
)
