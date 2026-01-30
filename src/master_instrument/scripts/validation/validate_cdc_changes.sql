-- ============================================================================
-- CDC Changes Validation Queries
-- ============================================================================
-- Use these queries to validate CDC workflow captures data correctly
-- Run after loading CDC changes to compare with master tables
-- ============================================================================

-- ============================================================================
-- MARKET DATA CDC VALIDATION
-- ============================================================================

-- 1. CDC operations summary
SELECT 
    sys_change_operation,
    COUNT(*) AS row_count,
    MIN(trade_date) AS min_date,
    MAX(trade_date) AS max_date
FROM public.tmp_int_market_data_changes
GROUP BY sys_change_operation;

-- 2. Sample check: compare all field values between CDC and master
SELECT 
    c.trade_date,
    c.quote_id,
    c.sys_change_operation,
    CASE WHEN m.quote_id IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS in_master,
    -- Value comparisons
    CASE WHEN c.currency_id = m.currency_id OR (c.currency_id IS NULL AND m.currency_id IS NULL) THEN '✓' ELSE '✗' END AS currency_ok,
    CASE WHEN c."open" = m."open" OR (c."open" IS NULL AND m."open" IS NULL) THEN '✓' ELSE '✗' END AS open_ok,
    CASE WHEN c.high = m.high OR (c.high IS NULL AND m.high IS NULL) THEN '✓' ELSE '✗' END AS high_ok,
    CASE WHEN c.low = m.low OR (c.low IS NULL AND m.low IS NULL) THEN '✓' ELSE '✗' END AS low_ok,
    CASE WHEN c."close" = m."close" OR (c."close" IS NULL AND m."close" IS NULL) THEN '✓' ELSE '✗' END AS close_ok,
    CASE WHEN c.volume = m.volume OR (c.volume IS NULL AND m.volume IS NULL) THEN '✓' ELSE '✗' END AS volume_ok,
    CASE WHEN c.bid = m.bid OR (c.bid IS NULL AND m.bid IS NULL) THEN '✓' ELSE '✗' END AS bid_ok,
    CASE WHEN c.ask = m.ask OR (c.ask IS NULL AND m.ask IS NULL) THEN '✓' ELSE '✗' END AS ask_ok,
    CASE WHEN c.vwap = m.vwap OR (c.vwap IS NULL AND m.vwap IS NULL) THEN '✓' ELSE '✗' END AS vwap_ok
FROM public.tmp_int_market_data_changes c
LEFT JOIN master.market_data m 
  ON c.trade_date = m.trade_date AND c.quote_id = m.quote_id
LIMIT 1000;


-- ============================================================================
-- COMPANY MARKET CAP CDC VALIDATION
-- ============================================================================

-- 1. CDC operations summary
SELECT 
    sys_change_operation,
    COUNT(*) AS row_count,
    MIN(valuation_date) AS min_date,
    MAX(valuation_date) AS max_date
FROM public.tmp_int_company_market_cap_changes
GROUP BY sys_change_operation;

-- 2. Sample check: compare all field values between CDC and master
SELECT 
    c.valuation_date,
    c.company_id,
    c.sys_change_operation,
    CASE WHEN m.company_id IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS in_master,
    -- Value comparisons
    CASE WHEN c.currency_id = m.currency_id OR (c.currency_id IS NULL AND m.currency_id IS NULL) THEN '✓' ELSE '✗' END AS currency_ok,
    CASE WHEN c.market_cap = m.market_cap OR (c.market_cap IS NULL AND m.market_cap IS NULL) THEN '✓' ELSE '✗' END AS market_cap_ok,
    CASE WHEN c.shares_outstanding = m.shares_outstanding OR (c.shares_outstanding IS NULL AND m.shares_outstanding IS NULL) THEN '✓' ELSE '✗' END AS shares_ok
FROM public.tmp_int_company_market_cap_changes c
LEFT JOIN master.company_market_cap m 
  ON c.valuation_date = m.valuation_date AND c.company_id = m.company_id
LIMIT 1000;


-- ============================================================================
-- STD FINANCIAL VALUE CDC VALIDATION
-- ============================================================================

-- 1. CDC operations summary
SELECT 
    sys_change_operation,
    COUNT(*) AS row_count,
    MIN(period_end_date) AS min_date,
    MAX(period_end_date) AS max_date
FROM public.tmp_int_std_financial_value_changes
GROUP BY sys_change_operation;

-- 2. Sample check: compare all field values between CDC and master
SELECT 
    c.company_id,
    c.std_financial_statement_id,
    c.std_financial_item_id,
    c.period_end_date,
    c.filing_end_date,
    c.sys_change_operation,
    CASE WHEN m.std_financial_value_id IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS in_master,
    -- Value comparisons
    CASE WHEN c.converted_value = m.value OR (c.converted_value IS NULL AND m.value IS NULL) THEN '✓' ELSE '✗' END AS value_ok,
    CASE WHEN c.conversion_factor = m.conversion_factor OR (c.conversion_factor IS NULL AND m.conversion_factor IS NULL) THEN '✓' ELSE '✗' END AS factor_ok
FROM public.tmp_int_std_financial_value_changes c
LEFT JOIN master.std_financial_value m 
  ON c.company_id = m.company_id 
 AND c.std_financial_statement_id = m.std_financial_statement_id
 AND c.std_financial_item_id = m.std_financial_item_id
LIMIT 1000;


-- ============================================================================
-- QUICK STATS
-- ============================================================================

-- Count rows in CDC temp tables
SELECT 'tmp_int_market_data_changes' AS table_name, COUNT(*) AS row_count 
FROM public.tmp_int_market_data_changes
UNION ALL
SELECT 'tmp_int_company_market_cap_changes', COUNT(*) 
FROM public.tmp_int_company_market_cap_changes
UNION ALL
SELECT 'tmp_int_std_financial_value_changes', COUNT(*) 
FROM public.tmp_int_std_financial_value_changes;

-- Count rows in master tables
SELECT 'master.market_data' AS table_name, COUNT(*) AS row_count 
FROM master.market_data
UNION ALL
SELECT 'master.company_market_cap', COUNT(*) 
FROM master.company_market_cap
UNION ALL
SELECT 'master.std_financial_value', COUNT(*) 
FROM master.std_financial_value;
