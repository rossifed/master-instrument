{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_instruments_without_quote"],
            "group": "data_quality",
            "description": "Identifies instruments that have no quote records. Every tradable instrument should have at least one quote representing a listing on an exchange."
        }
    }
) }}

-- Instruments without any quote
SELECT 
    i.instrument_id,
    i.entity_id,
    i.symbol,
    i.name AS instrument_name,
    e.name AS entity_name
FROM {{ source('master', 'instrument') }} i
JOIN {{ source('master', 'entity') }} e ON e.entity_id = i.entity_id
LEFT JOIN {{ source('master', 'quote') }} q ON q.instrument_id = i.instrument_id
WHERE q.quote_id IS NULL
