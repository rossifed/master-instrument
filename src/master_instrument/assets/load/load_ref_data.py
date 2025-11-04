# src/master_instrument/assets/load/instrument_id_mapping_asset.py

from dagster import asset
from master_instrument.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from master_instrument.alchemy.loaders.sql_file_loader import load_from_sql_file

@asset(key_prefix=["master"], group_name="load")
def equities(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_equities.sql")

@asset(key_prefix=["master"], group_name="load")
def companies(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_companies.sql")

@asset(key_prefix=["master"], group_name="load")
def countries(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_countries.sql")

@asset(key_prefix=["master"], group_name="load")
def currencies(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_currencies.sql")

@asset(key_prefix=["master"], group_name="load")
def entity_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_entity_types.sql")

@asset(key_prefix=["master"], group_name="load")
def instrument_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_instrument_types.sql")   

@asset(key_prefix=["master"], group_name="load")
def venue_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_venue_types.sql")   

@asset(key_prefix=["master"], group_name="load")
def quotes(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_quotes.sql")   
