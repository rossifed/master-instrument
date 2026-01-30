{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_instruments_without_primary_quote"],
            "group": "data_quality",
            "description": "Identifies instruments that have quotes but none marked as primary. Each instrument should have exactly one primary quote for consistent pricing."
        }
    }
) }}

-- Instruments with quotes but none is primary (shows all their quotes)
-- Uses NOT EXISTS for better performance
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
WHERE NOT EXISTS (
    SELECT 1 FROM {{ source('master', 'quote') }} q2 
    WHERE q2.instrument_id = i.instrument_id 
    AND q2.is_primary = true
)
ORDER BY i.instrument_id, q.quote_id
