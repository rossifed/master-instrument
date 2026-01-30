"""
Market data indexes for raw tables.

Tables: qa_DS2PrimQtPrc, qa_DS2PrimQtPrc_Changes, qa_DS2MktVal, qa_DS2MktVal_Changes,
        qa_DS2FxCode, qa_DS2FxRate, qa_DS2Div, qa_DS2Adj, qa_DS2CapEvent, qa_DS2NumShares,
        qa_DS2PrimQtRI, qa_DS2PrimQtRI_Changes
"""

from dagster import asset, Output, MetadataValue
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from .utils import create_indexes_for_table


DOMAIN = "market"


# =============================================================================
# qa_DS2PrimQtPrc - Market prices (high volume)
# =============================================================================
@asset(
    name="qa_DS2PrimQtPrc",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2PrimQtPrc (market prices)",
)
def raw_idx_ds2primqtprc(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2primqtprc_marketdate_as_date",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtprc_marketdate_as_date ON raw."qa_DS2PrimQtPrc" (("MarketDate"::date))'
        },
        {
            "name": "idx_ds2primqtprc_infocode_exchintcode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtprc_infocode_exchintcode ON raw."qa_DS2PrimQtPrc" ("InfoCode", "ExchIntCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2PrimQtPrc", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2PrimQtPrc_Changes - Incremental market data
# =============================================================================
@asset(
    name="qa_DS2PrimQtPrc_Changes",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2PrimQtPrc_Changes (incremental market data)",
)
def raw_idx_ds2primqtprc_changes(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2primqtprc_changes_version",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtprc_changes_version ON raw."qa_DS2PrimQtPrc_Changes" (sys_change_version)'
        },
        {
            "name": "idx_ds2primqtprc_changes_dedup",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtprc_changes_dedup ON raw."qa_DS2PrimQtPrc_Changes" ("InfoCode", "ExchIntCode", "MarketDate", sys_change_version DESC)'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2PrimQtPrc_Changes", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2MktVal - Market valuations
# =============================================================================
@asset(
    name="qa_DS2MktVal",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2MktVal (market valuations)",
)
def raw_idx_ds2mktval(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2mktval_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2mktval_infocode ON raw."qa_DS2MktVal" ("InfoCode")'
        },
        {
            "name": "idx_ds2mktval_valdate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2mktval_valdate ON raw."qa_DS2MktVal" ("ValDate")'
        },
        {
            "name": "idx_ds2mktval_valdate_as_date",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2mktval_valdate_as_date ON raw."qa_DS2MktVal" (("ValDate"::date))'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2MktVal", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2MktVal_Changes - Incremental valuations
# =============================================================================
@asset(
    name="qa_DS2MktVal_Changes",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2MktVal_Changes (incremental valuations)",
)
def raw_idx_ds2mktval_changes(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2mktval_changes_version",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2mktval_changes_version ON raw."qa_DS2MktVal_Changes" (sys_change_version)'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2MktVal_Changes", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2FxCode - FX code reference
# =============================================================================
@asset(
    name="qa_DS2FxCode",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2FxCode (FX codes)",
)
def raw_idx_ds2fxcode(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2fxcode_exrateint",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2fxcode_exrateint ON raw."qa_DS2FxCode" ("ExRateIntCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2FxCode", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2FxRate - FX rates
# =============================================================================
@asset(
    name="qa_DS2FxRate",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2FxRate (FX rates)",
)
def raw_idx_ds2fxrate(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2fxrate_exrateintcode_exratedate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2fxrate_exrateintcode_exratedate ON raw."qa_DS2FxRate" ("ExRateIntCode", "ExRateDate" DESC)'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2FxRate", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2Div - Dividends
# =============================================================================
@asset(
    name="qa_DS2Div",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2Div (dividends)",
)
def raw_idx_ds2div(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2div_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2div_infocode ON raw."qa_DS2Div" ("InfoCode")'
        },
        {
            "name": "idx_ds2div_effectivedate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2div_effectivedate ON raw."qa_DS2Div" ("EffectiveDate")'
        },
        {
            "name": "idx_ds2div_infocode_eventnum",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2div_infocode_eventnum ON raw."qa_DS2Div" ("InfoCode", "EventNum") WHERE "DivRate" IS NOT NULL AND "EffectiveDate" IS NOT NULL'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2Div", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2Adj - Adjustments
# =============================================================================
@asset(
    name="qa_DS2Adj",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2Adj (adjustments)",
)
def raw_idx_ds2adj(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2adj_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2adj_infocode ON raw."qa_DS2Adj" ("InfoCode")'
        },
        {
            "name": "idx_ds2adj_adjdate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2adj_adjdate ON raw."qa_DS2Adj" ("AdjDate")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2Adj", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2CapEvent - Capital events
# =============================================================================
@asset(
    name="qa_DS2CapEvent",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2CapEvent (capital events)",
)
def raw_idx_ds2capevent(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2capevent_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2capevent_infocode ON raw."qa_DS2CapEvent" ("InfoCode")'
        },
        {
            "name": "idx_ds2capevent_effectivedate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2capevent_effectivedate ON raw."qa_DS2CapEvent" ("EffectiveDate")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2CapEvent", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2NumShares - Shares outstanding
# =============================================================================
@asset(
    name="qa_DS2NumShares",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2NumShares (shares outstanding)",
)
def raw_idx_ds2numshares(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2numshares_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2numshares_infocode ON raw."qa_DS2NumShares" ("InfoCode")'
        },
        {
            "name": "idx_ds2numshares_eventdate",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2numshares_eventdate ON raw."qa_DS2NumShares" ("EventDate")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2NumShares", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2PrimQtRI - Total Return Index (high volume)
# =============================================================================
@asset(
    name="qa_DS2PrimQtRI",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2PrimQtRI (total return index)",
)
def raw_idx_ds2primqtri(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2primqtri_marketdate_as_date",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtri_marketdate_as_date ON raw."qa_DS2PrimQtRI" (("MarketDate"::date))'
        },
        {
            "name": "idx_ds2primqtri_infocode_exchintcode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtri_infocode_exchintcode ON raw."qa_DS2PrimQtRI" ("InfoCode", "ExchIntCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2PrimQtRI", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2PrimQtRI_Changes - Incremental Total Return Index
# =============================================================================
@asset(
    name="qa_DS2PrimQtRI_Changes",
    key_prefix=["infrastructure", "raw_indexes", "market"],
    group_name="infrastructure",
    description="Indexes for qa_DS2PrimQtRI_Changes (incremental total return index)",
)
def raw_idx_ds2primqtri_changes(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2primqtri_changes_marketdate_as_date",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtri_changes_marketdate_as_date ON raw."qa_DS2PrimQtRI_Changes" (("MarketDate"::date))'
        },
        {
            "name": "idx_ds2primqtri_changes_version",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtri_changes_version ON raw."qa_DS2PrimQtRI_Changes" (sys_change_version)'
        },
        {
            "name": "idx_ds2primqtri_changes_dedup",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2primqtri_changes_dedup ON raw."qa_DS2PrimQtRI_Changes" (("InfoCode"::text), ("ExchIntCode"::text), ("MarketDate"::date), sys_change_version DESC)'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2PrimQtRI_Changes", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })
