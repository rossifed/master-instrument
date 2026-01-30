-- =============================================================================
-- COVERAGE CHECKS - Master Data Validation
-- =============================================================================
-- This script validates coverage and consistency of master data.
-- Each test returns OK or FAIL with counts.
-- Compatible with DBeaver / any standard SQL client.
-- =============================================================================

-- =============================================================================
-- 1. VOLUME CHECKS - Minimum expected counts
-- =============================================================================

SELECT 
    'companies' AS entity,
    COUNT(*) AS actual_count,
    100000 AS min_expected,
    CASE WHEN COUNT(*) >= 100000 THEN 'OK' ELSE 'FAIL' END AS status
FROM master.company

UNION ALL

SELECT 
    'instruments' AS entity,
    COUNT(*) AS actual_count,
    60000 AS min_expected,
    CASE WHEN COUNT(*) >= 60000 THEN 'OK' ELSE 'FAIL' END AS status
FROM master.instrument

UNION ALL

SELECT 
    'quotes' AS entity,
    COUNT(*) AS actual_count,
    350000 AS min_expected,
    CASE WHEN COUNT(*) >= 350000 THEN 'OK' ELSE 'FAIL' END AS status
FROM master.quote;

-- =============================================================================
-- 2. MAPPING COVERAGE - 1:1 relationship checks
-- =============================================================================

WITH mapping_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM master.entity) AS entity_count,
        (SELECT COUNT(*) FROM master.entity_mapping) AS entity_mapping_count,
        (SELECT COUNT(*) FROM master.instrument) AS instrument_count,
        (SELECT COUNT(*) FROM master.instrument_mapping) AS instrument_mapping_count,
        (SELECT COUNT(*) FROM master.quote) AS quote_count,
        (SELECT COUNT(*) FROM master.quote_mapping) AS quote_mapping_count
)
SELECT 
    'entity vs entity_mapping' AS check_name,
    entity_count AS table_count,
    entity_mapping_count AS mapping_count,
    entity_count - entity_mapping_count AS diff,
    CASE WHEN entity_count = entity_mapping_count THEN 'OK' ELSE 'FAIL' END AS status
FROM mapping_counts

UNION ALL

SELECT 
    'instrument vs instrument_mapping' AS check_name,
    instrument_count AS table_count,
    instrument_mapping_count AS mapping_count,
    instrument_count - instrument_mapping_count AS diff,
    CASE WHEN instrument_count = instrument_mapping_count THEN 'OK' ELSE 'FAIL' END AS status
FROM mapping_counts

UNION ALL

SELECT 
    'quote vs quote_mapping' AS check_name,
    quote_count AS table_count,
    quote_mapping_count AS mapping_count,
    quote_count - quote_mapping_count AS diff,
    CASE WHEN quote_count = quote_mapping_count THEN 'OK' ELSE 'FAIL' END AS status
FROM mapping_counts;

-- =============================================================================
-- 3. TYPE COHERENCE - Subtype tables match typed entities
-- =============================================================================

WITH type_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM master.company) AS company_count,
        (SELECT COUNT(*) 
         FROM master.entity e 
         JOIN master.entity_type et ON et.entity_type_id = e.entity_type_id 
         WHERE et.mnemonic = 'COMPANY') AS entity_company_count,
        (SELECT COUNT(*) FROM master.equity) AS equity_count,
        (SELECT COUNT(*) 
         FROM master.instrument i 
         JOIN master.instrument_type it ON it.instrument_type_id = i.instrument_type_id 
         WHERE it.mnemonic = 'EQU') AS instrument_equity_count
)
SELECT 
    'company vs entity(COMPANY)' AS check_name,
    company_count AS subtype_count,
    entity_company_count AS typed_entity_count,
    company_count - entity_company_count AS diff,
    CASE WHEN company_count = entity_company_count THEN 'OK' ELSE 'FAIL' END AS status
FROM type_counts

UNION ALL

SELECT 
    'equity vs instrument(EQU)' AS check_name,
    equity_count AS subtype_count,
    instrument_equity_count AS typed_entity_count,
    equity_count - instrument_equity_count AS diff,
    CASE WHEN equity_count = instrument_equity_count THEN 'OK' ELSE 'FAIL' END AS status
FROM type_counts;

-- =============================================================================
-- 4. PRIMARY QUOTE UNIQUENESS - Each instrument has exactly 1 primary quote
-- =============================================================================

-- 4a. Instruments with NO primary quote
SELECT 
    'instruments_without_primary_quote' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAIL' END AS status
FROM master.instrument i
WHERE NOT EXISTS (
    SELECT 1 FROM master.quote q 
    WHERE q.instrument_id = i.instrument_id 
    AND q.is_primary = true
);

-- 4b. Instruments with MULTIPLE primary quotes
SELECT 
    'instruments_with_multiple_primary_quotes' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAIL' END AS status
FROM (
    SELECT q.instrument_id, COUNT(*) AS primary_count
    FROM master.quote q
    WHERE q.is_primary = true
    GROUP BY q.instrument_id
    HAVING COUNT(*) > 1
) multi;

-- 4c. Summary: instruments with exactly 1 primary quote
WITH primary_stats AS (
    SELECT 
        i.instrument_id,
        COUNT(q.quote_id) FILTER (WHERE q.is_primary = true) AS primary_count
    FROM master.instrument i
    LEFT JOIN master.quote q ON q.instrument_id = i.instrument_id
    GROUP BY i.instrument_id
)
SELECT 
    'instruments_with_exactly_1_primary' AS check_name,
    COUNT(*) FILTER (WHERE primary_count = 1) AS correct_count,
    COUNT(*) AS total_instruments,
    ROUND(100.0 * COUNT(*) FILTER (WHERE primary_count = 1) / NULLIF(COUNT(*), 0), 2) AS pct,
    CASE WHEN COUNT(*) FILTER (WHERE primary_count = 1) = COUNT(*) THEN 'OK' ELSE 'FAIL' END AS status
FROM primary_stats;

-- =============================================================================
-- 5. MARKET DATA PRESENCE - Primary quotes have price timeseries
-- =============================================================================

-- 5a. Primary quotes without any market data
SELECT 
    'primary_quotes_without_market_data' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'WARN' END AS status
FROM master.quote q
WHERE q.is_primary = true
AND NOT EXISTS (
    SELECT 1 FROM master.market_data md 
    WHERE md.quote_id = q.quote_id
);

-- 5b. Coverage ratio
WITH coverage AS (
    SELECT 
        COUNT(*) AS total_primary_quotes,
        COUNT(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM master.market_data md WHERE md.quote_id = q.quote_id
        )) AS with_market_data
    FROM master.quote q
    WHERE q.is_primary = true
)
SELECT 
    'primary_quote_market_data_coverage' AS check_name,
    with_market_data AS quotes_with_data,
    total_primary_quotes AS total_primary,
    ROUND(100.0 * with_market_data / NULLIF(total_primary_quotes, 0), 2) AS coverage_pct,
    CASE 
        WHEN with_market_data = total_primary_quotes THEN 'OK'
        WHEN with_market_data::float / NULLIF(total_primary_quotes, 0) >= 0.95 THEN 'WARN'
        ELSE 'FAIL' 
    END AS status
FROM coverage;
