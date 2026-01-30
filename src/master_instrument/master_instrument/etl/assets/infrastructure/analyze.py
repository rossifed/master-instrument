"""
Assets for running ANALYZE on all tables in raw, seed, and master schemas.
Ensures PostgreSQL has up-to-date statistics for query planning.
"""

import time
from typing import Any
from dagster import asset, Output, MetadataValue, get_dagster_logger, AssetKey
from sqlalchemy import text
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource


def analyze_schema_tables(engine: SqlAlchemyEngineResource, schema: str) -> dict[str, Any]:
    """
    Run ANALYZE on all tables in a schema.
    
    Args:
        engine: SQLAlchemy engine resource
        schema: Schema name (raw, seed, master)
        
    Returns:
        dict with analyzed tables and timing info
    """
    logger = get_dagster_logger()
    
    results: dict[str, Any] = {
        "schema": schema,
        "tables_analyzed": [],
        "tables_failed": [],
        "total_time_seconds": 0
    }
    
    start_total = time.time()
    
    with engine.get_engine().connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        
        # Get all tables in schema
        tables_query = text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = :schema
            ORDER BY tablename
        """)
        tables = conn.execute(tables_query, {"schema": schema}).fetchall()
        
        logger.info(f"[{schema}] Found {len(tables)} tables to analyze")
        
        for (table_name,) in tables:
            start = time.time()
            
            try:
                # Use quoted identifier for safety
                analyze_sql = text(f'ANALYZE {schema}."{table_name}"')
                conn.execute(analyze_sql)
                
                elapsed = time.time() - start
                logger.info(f"[{schema}] ✓ {table_name} analyzed in {elapsed:.2f}s")
                results["tables_analyzed"].append({
                    "table": table_name,
                    "time_seconds": round(elapsed, 2)
                })
                
            except Exception as e:
                elapsed = time.time() - start
                error_msg = str(e)
                logger.error(f"[{schema}] ✗ {table_name} FAILED: {error_msg}")
                results["tables_failed"].append({
                    "table": table_name,
                    "error": error_msg,
                    "time_seconds": round(elapsed, 2)
                })
    
    results["total_time_seconds"] = round(time.time() - start_total, 2)
    logger.info(f"[{schema}] Completed: {len(results['tables_analyzed'])} analyzed, {len(results['tables_failed'])} failed in {results['total_time_seconds']}s")
    
    return results


@asset(
    name="analyze_raw",
    key_prefix=["infrastructure", "maintenance"],
    group_name="infrastructure",
    description="Run ANALYZE on all tables in raw schema",
)
def analyze_raw_tables(engine: SqlAlchemyEngineResource) -> Output:
    """Analyze all tables in the raw schema for up-to-date statistics."""
    results = analyze_schema_tables(engine, "raw")
    return Output(value=results, metadata={
        "tables_analyzed": len(results["tables_analyzed"]),
        "tables_failed": len(results["tables_failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


@asset(
    name="analyze_seed",
    key_prefix=["infrastructure", "maintenance"],
    group_name="infrastructure",
    description="Run ANALYZE on all tables in seed schema",
)
def analyze_seed_tables(engine: SqlAlchemyEngineResource) -> Output:
    """Analyze all tables in the seed schema for up-to-date statistics."""
    results = analyze_schema_tables(engine, "seed")
    return Output(value=results, metadata={
        "tables_analyzed": len(results["tables_analyzed"]),
        "tables_failed": len(results["tables_failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


@asset(
    name="analyze_master",
    key_prefix=["infrastructure", "maintenance"],
    group_name="infrastructure",
    description="Run ANALYZE on all tables in master schema",
)
def analyze_master_tables(engine: SqlAlchemyEngineResource) -> Output:
    """Analyze all tables in the master schema for up-to-date statistics."""
    results = analyze_schema_tables(engine, "master")
    return Output(value=results, metadata={
        "tables_analyzed": len(results["tables_analyzed"]),
        "tables_failed": len(results["tables_failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


@asset(
    name="analyze_all",
    key_prefix=["infrastructure", "maintenance"],
    group_name="infrastructure",
    deps=[
        AssetKey(["infrastructure", "maintenance", "analyze_raw"]),
        AssetKey(["infrastructure", "maintenance", "analyze_seed"]),
        AssetKey(["infrastructure", "maintenance", "analyze_master"]),
    ],
    description="Run ANALYZE on all tables in raw, seed, and master schemas",
)
def analyze_all_schemas() -> Output:
    """Marker asset that depends on all schema analyze assets."""
    return Output(value={"status": "completed"}, metadata={
        "message": "All schemas analyzed",
    })
