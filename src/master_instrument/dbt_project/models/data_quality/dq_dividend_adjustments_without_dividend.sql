{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_dividend_adjustments_without_dividend"],
            "group": "data_quality",
            "description": "Identifies dividend adjustments that have no corresponding dividend record. May indicate orphan adjustment data."
        }
    }
) }}

-- Dividend adjustments without matching dividend
SELECT 
    da.equity_id,
    da.ex_div_date,
    da.cum_div_factor,
    i.name AS instrument_name
FROM {{ source('master', 'dividend_adjustment') }} da
JOIN {{ source('master', 'instrument') }} i ON i.instrument_id = da.equity_id
LEFT JOIN {{ source('master', 'dividend') }} d 
    ON d.equity_id = da.equity_id 
    AND d.effective_date = da.ex_div_date
WHERE d.dividend_id IS NULL
