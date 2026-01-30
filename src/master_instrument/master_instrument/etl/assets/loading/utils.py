"""Utility functions for loading assets."""

from dagster import get_dagster_logger
from sqlalchemy import text
from typing import Any

from master_instrument.etl.loading.loaders import SimpleLoader
from master_instrument.etl.loading.sources import SqlFileSource
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource


def load_from_sql(engine: SqlAlchemyEngineResource, sql_filename: str) -> None:
    """Simple SQL file execution without batching."""
    source = SqlFileSource(sql_filename)
    loader = SimpleLoader(engine.get_engine(), source, logger=get_dagster_logger())
    loader.load()


def delete_from_date(engine: SqlAlchemyEngineResource, table_name: str, from_date: str, date_column: str = "date") -> None:
    """Delete all rows from a specific date onwards (inclusive)."""
    logger = get_dagster_logger()
    logger.info(f"Deleting from {table_name} where {date_column} >= {from_date}...")
    
    with engine.get_engine().begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {table_name} WHERE {date_column} >= :from_date"),
            {"from_date": from_date}
        )
        rows_deleted = result.rowcount
    
    logger.info(f"✓ Deleted {rows_deleted:,} rows from {table_name} (>= {from_date})")


def truncate_table(engine: SqlAlchemyEngineResource, table_name: str) -> None:
    """Truncate a table with CASCADE."""
    logger = get_dagster_logger()
    logger.info(f"Truncating {table_name}...")
    
    with engine.get_engine().begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
    
    logger.info(f"Truncated {table_name}")


def drop_table_indexes(engine: SqlAlchemyEngineResource, schema: str, table: str) -> list[dict[str, Any]]:
    """Drop all non-PK indexes on a table. Returns list of dropped indexes for recreation."""
    logger = get_dagster_logger()
    
    with engine.get_engine().begin() as conn:
        result = conn.execute(text("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = :schema 
              AND tablename = :table
              AND indexname NOT LIKE 'pk_%'
              AND indexname NOT LIKE '%_pkey'
        """), {"schema": schema, "table": table})
        
        indexes = [{"name": row[0], "definition": row[1]} for row in result]
        
        if not indexes:
            logger.info(f"No secondary indexes to drop on {schema}.{table}")
            return []
        
        for idx in indexes:
            conn.execute(text(f"DROP INDEX IF EXISTS {schema}.{idx['name']}"))
        
        logger.info(f"Dropped {len(indexes)} indexes from {schema}.{table}")
        return indexes


def recreate_indexes(engine: SqlAlchemyEngineResource, indexes: list[dict[str, Any]]) -> None:
    """Recreate indexes from saved definitions."""
    logger = get_dagster_logger()
    
    if not indexes:
        return
    
    logger.info(f"Recreating {len(indexes)} indexes...")
    
    with engine.get_engine().begin() as conn:
        for idx in indexes:
            conn.execute(text(idx['definition']))
            logger.info(f"Created {idx['name']}")
    
    logger.info("Recreated all indexes")
