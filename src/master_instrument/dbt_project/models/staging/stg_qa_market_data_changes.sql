{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","market","stg_qa_market_data_changes"],
            "group": "market"
        }
    }
) }}

SELECT
    "MarketDate"::DATE AS date,
    "InfoCode"::TEXT AS info_code,
    "ExchIntCode"::TEXT AS exch_int_code,
    "InfoCode"::TEXT || '-' || "ExchIntCode"::TEXT AS external_quote_id,
    "ISOCurrCode" AS iso_curr_code,
    "Open_" AS open,
    "High" AS high,
    "Low" AS low,
    "Close_" AS close,
    "Volume" AS volume,
    "Bid" AS bid,
    "Ask" AS ask,
    "vwap" AS vwap,
    "sys_change_operation",
    "sys_change_version"::BIGINT AS sys_change_version,
    -- Type IDs from seeds (scalar subquery for optimizer)
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2PrimQtPrc_Changes') }}
WHERE "MarketDate" >= '{{ var('min_data_date') }}'::date
