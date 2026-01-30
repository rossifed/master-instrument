"""Database utilities for SQLAlchemy operations.

Functions for engine extraction, PostgreSQL operations (indexes, truncation),
and database-related utilities.
"""

from typing import List, Dict, Union, TYPE_CHECKING, TypeAlias, Optional
from contextlib import contextmanager
from sqlalchemy.engine import Engine
from sqlalchemy import text
from dagster import get_dagster_logger

if TYPE_CHECKING:
    from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource

EngineType: TypeAlias = Union[Engine, 'SqlAlchemyEngineResource']

Logger = Optional[object]


def extract_engine(engine_or_resource: EngineType) -> Engine:
    """Extract Engine from SqlAlchemyEngineResource or return Engine as-is.
    
    Args:
        engine_or_resource: Either Engine or SqlAlchemyEngineResource
        
    Returns:
        SQLAlchemy Engine instance
    """
    if hasattr(engine_or_resource, 'get_engine'):
        return engine_or_resource.get_engine()  # type: ignore
    return engine_or_resource  # type: ignore


def get_non_pk_indexes(engine: Engine, schema: str, table: str) -> List[Dict[str, str]]:
    """Get all non-PK indexes for a table.
    
    Excludes indexes that support constraints (PRIMARY KEY, UNIQUE) since those
    cannot be dropped directly - the constraint must be dropped instead.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        
    Returns:
        List of dicts with 'name' and 'definition' keys
    """
    # Exclude indexes that back constraints (PK, UNIQUE) - they can't be dropped directly
    query = text("""
        SELECT i.indexname, i.indexdef
        FROM pg_indexes i
        WHERE i.schemaname = :schema 
          AND i.tablename = :table
          AND NOT EXISTS (
              -- Exclude indexes that support a constraint
              SELECT 1 FROM pg_constraint c
              JOIN pg_class cl ON c.conrelid = cl.oid
              JOIN pg_namespace n ON cl.relnamespace = n.oid
              WHERE n.nspname = :schema
                AND cl.relname = :table
                AND c.conname = i.indexname
          )
    """)
    
    with engine.begin() as conn:
        result = conn.execute(query, {"schema": schema, "table": table})
        return [{"name": row[0], "definition": row[1]} for row in result]


def drop_indexes(engine: Engine, schema: str, indexes: List[Dict[str, str]]) -> None:
    """Drop indexes by name.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        indexes: List of index dicts with 'name' key
    """
    with engine.begin() as conn:
        for idx in indexes:
            conn.execute(text(f"DROP INDEX IF EXISTS {schema}.{idx['name']}"))


def recreate_indexes(engine: Engine, indexes: List[Dict[str, str]], concurrently: bool = True) -> None:
    """Recreate indexes from their definitions.

    Args:
        engine: SQLAlchemy engine
        indexes: List of index dicts with 'definition' key
        concurrently: If True, use CREATE INDEX CONCURRENTLY (reduces temp space usage, no locks)
    """
    for idx in indexes:
        # Extract definition and add CONCURRENTLY if requested
        definition = idx["definition"]

        if concurrently and "CREATE INDEX" in definition.upper() and "CONCURRENTLY" not in definition.upper():
            # Insert CONCURRENTLY after CREATE INDEX
            definition = definition.replace("CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1)

        # CONCURRENTLY requires autocommit (can't run in transaction)
        if concurrently:
            # Use raw connection with autocommit
            raw_conn = engine.raw_connection()
            try:
                raw_conn.set_isolation_level(0)  # AUTOCOMMIT mode
                cursor = raw_conn.cursor()
                cursor.execute(definition)
                cursor.close()
            finally:
                raw_conn.close()
        else:
            # Normal transaction mode
            with engine.begin() as conn:
                conn.execute(text(definition))


def truncate_table(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Truncate table with CASCADE."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Truncating {full_table}...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {full_table} CASCADE"))
    if logger:
        logger.info(f"✓ Truncated {full_table}")


def set_table_unlogged(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Set table to UNLOGGED mode (disables WAL)."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Setting {full_table} to UNLOGGED (WAL disabled)...")
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {full_table} SET UNLOGGED"))
    if logger:
        logger.info(f"✓ {full_table} set to UNLOGGED")


def set_table_logged(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Restore table to LOGGED mode (re-enables WAL)."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Restoring {full_table} to LOGGED (WAL enabled)...")
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {full_table} SET LOGGED"))
    if logger:
        logger.info(f"✓ {full_table} restored to LOGGED")


def disable_autovacuum(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Disable autovacuum for a table."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Disabling autovacuum on {full_table}...")
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {full_table} SET (autovacuum_enabled = false)"))
    if logger:
        logger.info(f"✓ Autovacuum disabled on {full_table}")


def enable_autovacuum(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Re-enable autovacuum for a table."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Re-enabling autovacuum on {full_table}...")
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {full_table} SET (autovacuum_enabled = true)"))
    if logger:
        logger.info(f"✓ Autovacuum re-enabled on {full_table}")


def analyze_table(engine: Engine, schema: str, table: str, logger: Logger = None) -> None:
    """Update table statistics for query planner."""
    full_table = f"{schema}.{table}"
    if logger:
        logger.info(f"Analyzing {full_table}...")
    with engine.begin() as conn:
        conn.execute(text(f"ANALYZE {full_table}"))
    if logger:
        logger.info(f"✓ Analyzed {full_table}")


# =============================================================================
# CONSTRAINT MANAGEMENT (FK, PK, UNIQUE)
# =============================================================================

def get_table_constraints(engine: Engine, schema: str, table: str) -> List[Dict[str, str]]:
    """Get all constraints for a table (FK, PK, UNIQUE, CHECK).
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        
    Returns:
        List of dicts with 'name', 'type', and 'definition' keys
        Type values: 'p' (PK), 'f' (FK), 'u' (UNIQUE), 'c' (CHECK)
    """
    query = text("""
        SELECT 
            con.conname AS name,
            con.contype AS type,
            pg_get_constraintdef(con.oid) AS definition
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relname = :table
        ORDER BY 
            CASE con.contype 
                WHEN 'p' THEN 1  -- PK first
                WHEN 'u' THEN 2  -- then UNIQUE
                WHEN 'f' THEN 3  -- then FK
                ELSE 4 
            END
    """)
    
    with engine.begin() as conn:
        result = conn.execute(query, {"schema": schema, "table": table})
        return [{"name": row[0], "type": row[1], "definition": row[2]} for row in result]


def get_foreign_keys(engine: Engine, schema: str, table: str) -> List[Dict[str, str]]:
    """Get only foreign key constraints for a table.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        
    Returns:
        List of dicts with 'name' and 'definition' keys
    """
    constraints = get_table_constraints(engine, schema, table)
    return [c for c in constraints if c["type"] == "f"]


def get_primary_key(engine: Engine, schema: str, table: str) -> Optional[Dict[str, str]]:
    """Get primary key constraint for a table.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        
    Returns:
        Dict with 'name' and 'definition' keys, or None if no PK
    """
    constraints = get_table_constraints(engine, schema, table)
    pks = [c for c in constraints if c["type"] == "p"]
    return pks[0] if pks else None


def get_unique_constraints(engine: Engine, schema: str, table: str) -> List[Dict[str, str]]:
    """Get unique constraints for a table (not including PK).
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        
    Returns:
        List of dicts with 'name' and 'definition' keys
    """
    constraints = get_table_constraints(engine, schema, table)
    return [c for c in constraints if c["type"] == "u"]


def drop_constraints(
    engine: Engine, 
    schema: str, 
    table: str, 
    constraints: List[Dict[str, str]], 
    logger: Logger = None
) -> None:
    """Drop constraints by name.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        constraints: List of constraint dicts with 'name' key
        logger: Optional logger
    """
    full_table = f"{schema}.{table}"
    with engine.begin() as conn:
        for con in constraints:
            if logger:
                logger.info(f"Dropping constraint {con['name']} on {full_table}...")
            conn.execute(text(f"ALTER TABLE {full_table} DROP CONSTRAINT IF EXISTS {con['name']}"))
            if logger:
                logger.info(f"✓ Dropped {con['name']}")


def recreate_constraints(
    engine: Engine, 
    schema: str, 
    table: str, 
    constraints: List[Dict[str, str]], 
    not_valid: bool = True,
    logger: Logger = None
) -> None:
    """Recreate constraints from their definitions.
    
    Args:
        engine: SQLAlchemy engine
        schema: Schema name
        table: Table name
        constraints: List of constraint dicts with 'name', 'type', and 'definition' keys
        not_valid: If True, create FK constraints as NOT VALID (faster, validates on new rows only)
        logger: Optional logger
    """
    full_table = f"{schema}.{table}"
    
    with engine.begin() as conn:
        for con in constraints:
            name = con["name"]
            definition = con["definition"]
            con_type = con.get("type", "")
            
            # Build ALTER TABLE statement
            sql = f"ALTER TABLE {full_table} ADD CONSTRAINT {name} {definition}"
            
            # Add NOT VALID for FK constraints if requested (much faster)
            if not_valid and con_type == "f" and "NOT VALID" not in sql.upper():
                sql += " NOT VALID"
            
            if logger:
                logger.info(f"Recreating constraint {name} on {full_table}...")
            
            conn.execute(text(sql))
            
            if logger:
                logger.info(f"✓ Recreated {name}")


@contextmanager
def bulk_load_mode(engine: Engine, schema: str, table: str, disable_wal: bool = False, disable_vacuum: bool = False, logger: Logger = None):
    """Context manager for bulk load optimizations."""
    try:
        if disable_vacuum:
            disable_autovacuum(engine, schema, table, logger)

        if disable_wal:
            set_table_unlogged(engine, schema, table, logger)

        yield

    finally:
        if disable_wal:
            set_table_logged(engine, schema, table, logger)

        if disable_vacuum:
            enable_autovacuum(engine, schema, table, logger)

        analyze_table(engine, schema, table, logger)
