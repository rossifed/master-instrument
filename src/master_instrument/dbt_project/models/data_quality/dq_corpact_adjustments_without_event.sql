{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_corpact_adjustments_without_event"],
            "group": "data_quality",
            "description": "Identifies corporate action adjustments that have no corresponding event record."
        }
    }
) }}

-- Corporate action adjustments without matching event
SELECT 
    ca.equity_id,
    ca.adj_date,
    ca.adj_type,
    ca.cum_adj_factor,
    i.name AS instrument_name
FROM {{ source('master', 'corpact_adjustment') }} ca
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = ca.equity_id
LEFT JOIN {{ source('master', 'corpact_event') }} ce 
    ON ce.equity_id = ca.equity_id 
    AND ce.effective_date = ca.adj_date
WHERE ce.corpact_event_id IS NULL
