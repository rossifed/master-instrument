-- =============================================================================
-- SNAPSHOT TABLE COUNTS - Master Schema
-- =============================================================================
-- Usage:
--   1. Run before a job: save the result
--   2. Run after the job: compare with the previous snapshot
--   3. If counts change without new source data = problem
-- =============================================================================

SELECT 
    table_name,
    row_count,
    NOW() AS snapshot_time
FROM (
    SELECT 'entity' AS table_name, COUNT(*) AS row_count FROM master.entity
    UNION ALL SELECT 'entity_type', COUNT(*) FROM master.entity_type
    UNION ALL SELECT 'entity_mapping', COUNT(*) FROM master.entity_mapping
    UNION ALL SELECT 'company', COUNT(*) FROM master.company
    UNION ALL SELECT 'company_weblink', COUNT(*) FROM master.company_weblink
    UNION ALL SELECT 'company_market_cap', COUNT(*) FROM master.company_market_cap
    UNION ALL SELECT 'instrument', COUNT(*) FROM master.instrument
    UNION ALL SELECT 'instrument_type', COUNT(*) FROM master.instrument_type
    UNION ALL SELECT 'instrument_mapping', COUNT(*) FROM master.instrument_mapping
    UNION ALL SELECT 'equity', COUNT(*) FROM master.equity
    UNION ALL SELECT 'equity_type', COUNT(*) FROM master.equity_type
    UNION ALL SELECT 'quote', COUNT(*) FROM master.quote
    UNION ALL SELECT 'quote_mapping', COUNT(*) FROM master.quote_mapping
    UNION ALL SELECT 'venue', COUNT(*) FROM master.venue
    UNION ALL SELECT 'venue_type', COUNT(*) FROM master.venue_type
    UNION ALL SELECT 'venue_mapping', COUNT(*) FROM master.venue_mapping
    UNION ALL SELECT 'market_data', COUNT(*) FROM master.market_data
    UNION ALL SELECT 'currency', COUNT(*) FROM master.currency
    UNION ALL SELECT 'currency_pair', COUNT(*) FROM master.currency_pair
    UNION ALL SELECT 'country', COUNT(*) FROM master.country
    UNION ALL SELECT 'region', COUNT(*) FROM master.region
    UNION ALL SELECT 'country_region', COUNT(*) FROM master.country_region
    UNION ALL SELECT 'fx_rate', COUNT(*) FROM master.fx_rate
    UNION ALL SELECT 'dividend', COUNT(*) FROM master.dividend
    UNION ALL SELECT 'dividend_type', COUNT(*) FROM master.dividend_type
    UNION ALL SELECT 'dividend_adjustment', COUNT(*) FROM master.dividend_adjustment
    UNION ALL SELECT 'corpact_event', COUNT(*) FROM master.corpact_event
    UNION ALL SELECT 'corpact_type', COUNT(*) FROM master.corpact_type
    UNION ALL SELECT 'corpact_adjustment', COUNT(*) FROM master.corpact_adjustment
    UNION ALL SELECT 'share_outstanding', COUNT(*) FROM master.share_outstanding
    UNION ALL SELECT 'std_financial_filing', COUNT(*) FROM master.std_financial_filing
    UNION ALL SELECT 'std_financial_statement', COUNT(*) FROM master.std_financial_statement
    UNION ALL SELECT 'std_financial_value', COUNT(*) FROM master.std_financial_value
    UNION ALL SELECT 'std_financial_item', COUNT(*) FROM master.std_financial_item
    UNION ALL SELECT 'std_financial_item_mapping', COUNT(*) FROM master.std_financial_item_mapping
    UNION ALL SELECT 'financial_period_type', COUNT(*) FROM master.financial_period_type
    UNION ALL SELECT 'financial_statement_type', COUNT(*) FROM master.financial_statement_type
    UNION ALL SELECT 'classification_scheme', COUNT(*) FROM master.classification_scheme
    UNION ALL SELECT 'classification_level', COUNT(*) FROM master.classification_level
    UNION ALL SELECT 'classification_node', COUNT(*) FROM master.classification_node
    UNION ALL SELECT 'entity_classification', COUNT(*) FROM master.entity_classification
    UNION ALL SELECT 'weblink_type', COUNT(*) FROM master.weblink_type
    UNION ALL SELECT 'data_source', COUNT(*) FROM master.data_source
) counts
ORDER BY table_name;
