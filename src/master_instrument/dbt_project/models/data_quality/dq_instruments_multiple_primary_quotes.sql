{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_instruments_multiple_primary_quotes"],
            "group": "data_quality",
            "description": "Identifies instruments with multiple quotes marked as primary. Each instrument should have exactly one primary quote."
        }
    }
) }}

-- Instruments with multiple primary quotes
SELECT 
    i.instrument_id,
    i.entity_id,
    i.symbol,
    i.name AS instrument_name,
    q.quote_id,
    q.ric,
    q.ticker,
    q.is_primary
FROM {{ source('master', 'instrument') }} i
JOIN {{ source('master', 'quote') }} q ON q.instrument_id = i.instrument_id
WHERE q.is_primary = TRUE
  AND i.instrument_id IN (
    SELECT q2.instrument_id
    FROM {{ source('master', 'quote') }} q2
    WHERE q2.is_primary = TRUE
    GROUP BY q2.instrument_id
    HAVING COUNT(*) > 1
)
ORDER BY i.instrument_id, q.quote_id
