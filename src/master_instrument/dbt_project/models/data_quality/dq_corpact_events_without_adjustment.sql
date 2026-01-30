{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_corpact_events_without_adjustment"],
            "group": "data_quality",
            "description": "Identifies corporate action events that have no corresponding adjustment record."
        }
    }
) }}

-- Corporate action events without any adjustment
SELECT 
    ce.corpact_event_id,
    ce.equity_id,
    i.name AS instrument_name,
    ct.description AS corpact_type,
    ce.effective_date
FROM {{ source('master', 'corpact_event') }} ce
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = ce.equity_id
JOIN {{ source('master', 'corpact_type') }} ct ON ct.corpact_type_id = ce.corpact_type_id
LEFT JOIN {{ source('master', 'corpact_adjustment') }} ca 
    ON ca.equity_id = ce.equity_id 
    AND ca.adj_date = ce.effective_date
WHERE ca.equity_id IS NULL
