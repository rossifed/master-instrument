{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_quotes_without_market_data"],
            "group": "data_quality",
            "description": "Identifies quotes that have no market data records. Uses optimized anti-join pattern for TimescaleDB hypertables. Limited to 100 rows for performance."
        }
    }
) }}

-- Quotes/instruments without any market data
-- Uses CTE anti-join pattern optimized for TimescaleDB hypertables
WITH quotes_with_data AS (
    SELECT DISTINCT quote_id 
    FROM {{ source('master', 'market_data') }}
)
SELECT 
    q.quote_id,
    q.ric,
    q.ticker,
    i.instrument_id,
    i.entity_id,
    i.name AS instrument_name
FROM {{ source('master', 'quote') }} q
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = q.instrument_id
LEFT JOIN quotes_with_data qwd ON qwd.quote_id = q.quote_id
WHERE qwd.quote_id IS NULL
LIMIT 100
