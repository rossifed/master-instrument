{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_dividends_without_adjustment"],
            "group": "data_quality",
            "description": "Identifies dividends that have no corresponding adjustment record. May indicate missing price adjustment data."
        }
    }
) }}

-- Dividends without any adjustment
SELECT 
    d.dividend_id,
    d.equity_id,
    i.name AS instrument_name,
    dt.description AS dividend_type,
    d.effective_date,
    d.dividend_rate
FROM {{ source('master', 'dividend') }} d
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = d.equity_id
JOIN {{ source('master', 'dividend_type') }} dt ON dt.dividend_type_id = d.dividend_type_id
LEFT JOIN {{ source('master', 'dividend_adjustment') }} da 
    ON da.equity_id = d.equity_id 
    AND da.ex_div_date = d.effective_date
WHERE da.equity_id IS NULL
