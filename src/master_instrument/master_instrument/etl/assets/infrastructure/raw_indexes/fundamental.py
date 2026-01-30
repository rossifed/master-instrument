"""
Fundamental data indexes for raw tables.

Tables: qa_RKDFndStdFinVal, qa_RKDFndStdFinVal_Changes, qa_RKDFndStdPerFiling,
        qa_RKDFndStdPeriod, qa_RKDFndStdStmt, qa_RKDFndStdItem, qa_RKDFndCode
"""

from dagster import asset, Output, MetadataValue
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from .utils import create_indexes_for_table


DOMAIN = "fundamental"


# =============================================================================
# qa_RKDFndStdFinVal - Financial values (high volume)
# =============================================================================
@asset(
    name="qa_RKDFndStdFinVal",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdFinVal (financial values)",
)
def raw_idx_rkdfndstdfinval(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstdfinval_code_pertypecode_perenddt_stmtdt_item",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdfinval_code_pertypecode_perenddt_stmtdt_item ON raw."qa_RKDFndStdFinVal" ("Code", "PerTypeCode", "PerEndDt", "StmtDt", "Item")'
        },
        {
            "name": "idx_rkdfndstdfinval_perenddt",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdfinval_perenddt ON raw."qa_RKDFndStdFinVal" ("PerEndDt")'
        },
        {
            "name": "idx_rkdfndstdfinval_item",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdfinval_item ON raw."qa_RKDFndStdFinVal" ("Item")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdFinVal", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndStdFinVal_Changes - Incremental financial values
# =============================================================================
@asset(
    name="qa_RKDFndStdFinVal_Changes",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdFinVal_Changes (incremental financial values)",
)
def raw_idx_rkdfndstdfinval_changes(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstdfinval_changes_version",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdfinval_changes_version ON raw."qa_RKDFndStdFinVal_Changes" (sys_change_version)'
        },
        {
            "name": "idx_rkdfndstdfinval_changes_code_pertypecode_perenddt_stmtdt",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdfinval_changes_code_pertypecode_perenddt_stmtdt ON raw."qa_RKDFndStdFinVal_Changes" ("Code", "PerTypeCode", "PerEndDt", "StmtDt")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdFinVal_Changes", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndStdPerFiling - Period filings
# =============================================================================
@asset(
    name="qa_RKDFndStdPerFiling",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdPerFiling (period filings)",
)
def raw_idx_rkdfndstdperfiling(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstdperfiling_code_pertypecode_perenddt_stmtdt",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdperfiling_code_pertypecode_perenddt_stmtdt ON raw."qa_RKDFndStdPerFiling" ("Code", "PerTypeCode", "PerEndDt", "StmtDt")'
        },
        {
            "name": "idx_rkdfndstdperfiling_code_text",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdperfiling_code_text ON raw."qa_RKDFndStdPerFiling" (("Code")::text)'
        },
        {
            "name": "idx_rkdfndstdperfiling_perend_code",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdperfiling_perend_code ON raw."qa_RKDFndStdPerFiling" ("PerEndDt", "Code", "PerTypeCode", "StmtDt")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdPerFiling", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndStdPeriod - Periods
# =============================================================================
@asset(
    name="qa_RKDFndStdPeriod",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdPeriod (periods)",
)
def raw_idx_rkdfndstdperiod(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstdperiod_code_pertypecode_perenddt",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdperiod_code_pertypecode_perenddt ON raw."qa_RKDFndStdPeriod" ("Code", "PerTypeCode", "PerEndDt")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdPeriod", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndStdStmt - Statements
# =============================================================================
@asset(
    name="qa_RKDFndStdStmt",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdStmt (statements)",
)
def raw_idx_rkdfndstdstmt(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstdstmt_code_pertypecode_perenddt_stmtdt",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstdstmt_code_pertypecode_perenddt_stmtdt ON raw."qa_RKDFndStdStmt" ("Code", "PerTypeCode", "PerEndDt", "StmtDt")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdStmt", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndStdItem - Standard items
# =============================================================================
@asset(
    name="qa_RKDFndStdItem",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndStdItem (standard items)",
)
def raw_idx_rkdfndstditem(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndstditem_item",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndstditem_item ON raw."qa_RKDFndStdItem" ("Item")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndStdItem", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndCode - Code lookups (used everywhere)
# =============================================================================
@asset(
    name="qa_RKDFndCode",
    key_prefix=["infrastructure", "raw_indexes", "fundamental"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndCode (code lookups)",
)
def raw_idx_rkdfndcode(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndcode_code_type",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcode_code_type ON raw."qa_RKDFndCode" ("Code", "Type_")'
        },
        {
            "name": "idx_rkdfndcode_curr_lookup",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcode_curr_lookup ON raw."qa_RKDFndCode" ("Code", "Type_") WHERE "Type_" = 58'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndCode", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })
