{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_share_outstanding_gaps"],
            "group": "data_quality"
        }
    }
) }}

-- 5.2 Share outstanding with >20% gaps
WITH date_range AS (
    SELECT 
        equity_id,
        MIN(date) AS min_date,
        MAX(date) AS max_date,
        COUNT(*) AS actual_days
    FROM {{ source('master', 'share_outstanding') }}
    GROUP BY equity_id
),
expected AS (
    SELECT 
        dr.equity_id,
        dr.min_date,
        dr.max_date,
        dr.actual_days,
        GREATEST(1, (dr.max_date - dr.min_date) * 5 / 7) AS expected_days
    FROM date_range dr
)
SELECT 
    e.equity_id,
    i.name AS instrument_name,
    e.min_date,
    e.max_date,
    e.actual_days,
    e.expected_days,
    ROUND(100.0 * (1 - e.actual_days::numeric / e.expected_days), 2) AS gap_percentage
FROM expected e
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = e.equity_id
WHERE e.expected_days > 0
  AND (1 - e.actual_days::numeric / e.expected_days) > 0.20
