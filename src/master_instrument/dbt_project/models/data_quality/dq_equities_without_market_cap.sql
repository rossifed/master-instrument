{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_equities_without_market_cap"],
            "group": "data_quality",
            "description": "Identifies equities whose parent company has no market capitalization data."
        }
    }
) }}

-- Equities without any market cap data (via company)
SELECT 
    e.equity_id,
    i.name AS instrument_name,
    ent.entity_id,
    ent.name AS entity_name
FROM {{ source('master', 'equity') }} e
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = e.equity_id
JOIN {{ source('master', 'entity') }} ent ON ent.entity_id = i.entity_id
JOIN {{ source('master', 'company') }} c ON c.company_id = ent.entity_id
LEFT JOIN {{ source('master', 'company_market_cap') }} cmc ON cmc.company_id = c.company_id
WHERE cmc.company_id IS NULL
