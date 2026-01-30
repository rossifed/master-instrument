# src/master_instrument/resources/sqlalchemy_resource.py
from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class SqlAlchemyEngineResource(ConfigurableResource):
    connection_string: str  # loaded via config, .env, etc.

    def get_engine(self) -> Engine:
        return create_engine(self.connection_string)
