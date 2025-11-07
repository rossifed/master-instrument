from dagster import asset
from master_instrument.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from master_instrument.alchemy.loaders.sql_file_loader import load_from_sql_file

@asset(key_prefix=["master"], group_name="ref_data", deps=["equity_types","currencies","countries", "companies"])
def equities(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_equities.sql")

@asset(key_prefix=["master"], group_name="ref_data",deps=["entity_types"])
def companies(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_companies.sql")

@asset(key_prefix=["master"], group_name="ref_data")
def countries(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_countries.sql")

@asset(key_prefix=["master"], group_name="ref_data")
def currencies(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_currencies.sql")

@asset(key_prefix=["master"], group_name="ref_data")
def entity_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_entity_types.sql")

@asset(key_prefix=["master"], group_name="ref_data")
def instrument_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_instrument_types.sql")   

@asset(key_prefix=["master"], group_name="ref_data")
def venue_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_venue_types.sql")   

@asset(key_prefix=["master"], group_name="ref_data" ,deps=["venues", "equities", "currencies"])
def quotes(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_quotes.sql")   


@asset(key_prefix=["master"], group_name="ref_data",deps=["venue_types", "countries", "currencies"])
def venues(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_venues.sql")  

@asset(key_prefix=["master"], group_name="ref_data")
def equity_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_equity_types.sql")   

@asset(key_prefix=["master"], group_name="ref_data",deps=["weblink_types"])
def company_weblinks(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_company_weblinks.sql")   

@asset(key_prefix=["master"], group_name="ref_data")
def weblink_types(engine: SqlAlchemyEngineResource) -> None:
    load_from_sql_file(engine.get_engine(), "load_weblink_types.sql")  
