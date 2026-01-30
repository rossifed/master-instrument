"""
Post-load hooks for Sling assets.

This module contains SQL operations to execute after Sling data loads,
such as creating performance indexes on raw tables.
"""

from dagster import get_dagster_logger
from dagster_sling import DagsterSlingTranslator
from sqlalchemy import text


def create_market_data_indexes(context, engine):
    """
    Create performance indexes on market data raw tables after Sling loads.
    
    Covers: DS2PrimQtPrc, DS2FxRate
    """
    logger = get_dagster_logger()
    
    indexes = [
        {
            "name": "idx_ds2primqtprc_infocode_marketdate",
            "table": 'raw."qa_DS2PrimQtPrc"',
            "columns": '("InfoCode", "MarketDate" DESC)',
            "description": "Find last price before ex-dividend date"
        },
        {
            "name": "idx_fxrate_exrateint_exratedate",
            "table": 'raw."qa_DS2FxRate"',
            "columns": '("ExRateIntCode", "ExRateDate" DESC)',
            "description": "Find last FX rate before price date"
        }
    ]
    
    _create_indexes(logger, engine, indexes)


def create_referential_indexes(context, engine):
    """
    Create performance indexes on referential raw tables after Sling loads.
    
    Covers: DS2Div, DS2FxCode
    """
    logger = get_dagster_logger()
    
    indexes = [
        {
            "name": "idx_fxcode_exrateint",
            "table": 'raw."qa_DS2FxCode"',
            "columns": '("ExRateIntCode")',
            "description": "Join FX code to FX rate tables"
        },
        {
            "name": "idx_ds2div_infocode_effectivedate",
            "table": 'raw."qa_DS2Div"',
            "columns": '("InfoCode", "EffectiveDate")',
            "description": "Filter dividends by security and date"
        },
        {
            "name": "idx_ds2div_infocode",
            "table": 'raw."qa_DS2Div"',
            "columns": '("InfoCode")',
            "description": "JOIN with instrument_mapping"
        },
        {
            "name": "idx_ds2div_divtypecode",
            "table": 'raw."qa_DS2Div"',
            "columns": '("DivTypeCode")',
            "description": "JOIN with dividend_type"
        },
        {
            "name": "idx_ds2div_isocurrcode",
            "table": 'raw."qa_DS2Div"',
            "columns": '("ISOCurrCode")',
            "description": "JOIN with currency"
        },
        {
            "name": "idx_ds2div_infocode_eventnum",
            "table": 'raw."qa_DS2Div"',
            "columns": '("InfoCode", "EventNum")',
            "where": '"DivRate" IS NOT NULL AND "EffectiveDate" IS NOT NULL',
            "description": "Composite with WHERE filter for MERGE"
        }
    ]
    
    _create_indexes(logger, engine, indexes)


def _create_indexes(logger, engine, indexes):
    """
    Internal helper to create indexes.
    
    Executes CREATE INDEX CONCURRENTLY to avoid blocking table access.
    Failures are logged but don't fail the asset materialization.
    """
    with engine.connect() as conn:
        for idx in indexes:
            try:
                where_clause = f" WHERE {idx['where']}" if idx.get('where') else ""
                sql = f"""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx['name']}
                    ON {idx['table']} {idx['columns']}{where_clause}
                """
                
                logger.info(f"Creating index {idx['name']} - {idx['description']}")
                conn.execute(text(sql))
                conn.commit()
                
            except Exception as e:
                # Log but don't fail - indexes are performance optimization
                logger.warning(f"Failed to create index {idx['name']}: {str(e)}")
