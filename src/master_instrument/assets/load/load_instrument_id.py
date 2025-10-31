# src/master_instrument/assets/load/instrument_id_mapping_asset.py

from dagster import asset
from master_instrument.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from master_instrument.alchemy.loaders.instrument_id_loader import load_from_orm, load_from_sql

@asset(key_prefix=["master"], group_name="load")
def instrument_id_mapping_load_sql(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql(engine.get_engine())


@asset(key_prefix=["master"], group_name="load")
def instrument_id_mapping_load_orm(engine: SqlAlchemyEngineResource) -> None:
    load_from_orm(engine.get_engine())
