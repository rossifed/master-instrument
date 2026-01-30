-- Regression test: Market Cap Alphabet (Google)
-- Reference values validated on 2026-01-15 against internal reference system
-- company_id: 5378 (Alphabet Inc.)
-- Goal: Detect mapping regressions (e.g., InfoCode → rkd_code bug)
-- 
-- Tolerance: 0.01% (values must match within 0.01%)
-- This test fails if any value diverges by more than 0.01% from reference

WITH reference_values AS (
    -- Reference value snapshot (in millions USD)
    -- Source: Internal reference system, validated on 2026-01-15
    SELECT * FROM (VALUES
        (5378, '2026-01-12'::DATE, 4009257.0),
        (5378, '2026-01-09'::DATE, 3967933.0),
        (5378, '2026-01-08'::DATE, 3930164.0),
        (5378, '2026-01-07'::DATE, 3887764.0),
        (5378, '2026-01-06'::DATE, 3794274.0),
        (5378, '2026-01-05'::DATE, 3823903.0),
        (5378, '2026-01-02'::DATE, 3803831.0),
        (5378, '2026-01-01'::DATE, 3781294.0),
        (5378, '2025-12-31'::DATE, 3781294.0),
        (5378, '2025-12-30'::DATE, 3791011.0),
        (5378, '2025-12-29'::DATE, 3788214.0)
    ) AS t(company_id, valuation_date, expected_market_cap)
),

actual_values AS (
    SELECT 
        company_id,
        valuation_date,
        market_cap
    FROM {{ source('master', 'company_market_cap') }}
    WHERE company_id = 5378
      AND valuation_date >= '2025-12-29'
      AND valuation_date <= '2026-01-12'
)

-- Test fails if any rows are returned (diff > 0.01%)
SELECT 
    r.company_id,
    r.valuation_date,
    r.expected_market_cap,
    a.market_cap AS actual_market_cap,
    CASE 
        WHEN a.market_cap IS NULL THEN 'MISSING'
        ELSE ROUND((ABS(r.expected_market_cap - a.market_cap) / r.expected_market_cap * 100)::NUMERIC, 4)::TEXT || '%'
    END AS pct_diff
FROM reference_values r
LEFT JOIN actual_values a 
    ON a.company_id = r.company_id 
    AND a.valuation_date = r.valuation_date
WHERE a.market_cap IS NULL  -- Missing value
   OR ABS(r.expected_market_cap - a.market_cap) / r.expected_market_cap > 0.0001  -- Diff > 0.01%
