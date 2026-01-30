"""
Utility functions for creating raw indexes.
"""

import time
from typing import Any
from dagster import get_dagster_logger
from sqlalchemy import text
from sqlalchemy.engine import Engine


def create_indexes_for_table(engine: Engine, table: str, indexes: list[dict[str, str]], domain: str) -> dict[str, Any]:
    """
    Create indexes for a single table with detailed logging and timing.
    
    Args:
        engine: SQLAlchemy engine
        table: Table name for logging
        indexes: List of dicts with 'name', 'sql' keys
        domain: Domain name for logging
        
    Returns:
        dict with created, skipped, failed counts and details
    """
    logger = get_dagster_logger()
    
    results: dict[str, Any] = {
        "table": table,
        "domain": domain,
        "created": [],
        "skipped": [],
        "failed": [],
        "total_time_seconds": 0
    }
    
    created: list[dict[str, Any]] = results["created"]
    skipped: list[dict[str, str]] = results["skipped"]
    failed: list[dict[str, Any]] = results["failed"]
    
    start_total = time.time()
    
    conn = engine.connect()
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    
    try:
        for idx in indexes:
            idx_name = idx["name"]
            idx_sql = idx["sql"]
            
            logger.info(f"[{domain}/{table}] Creating index {idx_name}...")
            start = time.time()
            
            try:
                conn.execute(text(idx_sql))
                elapsed = time.time() - start
                
                logger.info(f"[{domain}/{table}] ✓ {idx_name} completed in {elapsed:.2f}s")
                created.append({
                    "name": idx_name,
                    "time_seconds": round(elapsed, 2)
                })
                
            except Exception as e:
                elapsed = time.time() - start
                error_msg = str(e)
                
                if "already exists" in error_msg.lower():
                    logger.info(f"[{domain}/{table}] → {idx_name} already exists (skipped)")
                    skipped.append({"name": idx_name})
                else:
                    logger.error(f"[{domain}/{table}] ✗ {idx_name} FAILED after {elapsed:.2f}s: {error_msg}")
                    failed.append({
                        "name": idx_name,
                        "error": error_msg,
                        "time_seconds": round(elapsed, 2)
                    })
    finally:
        conn.close()
    
    results["total_time_seconds"] = round(time.time() - start_total, 2)
    
    # Summary log
    logger.info(
        f"[{domain}/{table}] Complete: "
        f"{len(created)} created, "
        f"{len(skipped)} skipped, "
        f"{len(failed)} failed "
        f"in {results['total_time_seconds']:.2f}s"
    )
    
    return results
