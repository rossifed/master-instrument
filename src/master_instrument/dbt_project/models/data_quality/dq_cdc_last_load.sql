{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_cdc_last_load"],
            "group": "data_quality",
            "description": "Shows the last CDC load for each timeseries table (market_data, company_market_cap, std_financial_value)."
        }
    }
) }}

-- Last CDC load for each timeseries table
-- Useful for quick check after incremental loads

(
    SELECT 
        'market_data' AS entity,
        last_source_version,
        loaded_at,
        rows_inserted,
        rows_updated,
        rows_deleted
    FROM master.market_data_load
    ORDER BY loaded_at DESC
    LIMIT 1
)

UNION ALL

(
    SELECT 
        'company_market_cap' AS entity,
        last_source_version,
        loaded_at,
        rows_inserted,
        rows_updated,
        rows_deleted
    FROM master.company_market_cap_load
    ORDER BY loaded_at DESC
    LIMIT 1
)

UNION ALL

(
    SELECT 
        'std_financial_value' AS entity,
        last_source_version,
        loaded_at,
        rows_inserted,
        rows_updated,
        rows_deleted
    FROM master.std_financial_value_load
    ORDER BY loaded_at DESC
    LIMIT 1
)
