{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","market","stg_qa_total_return_changes"],
            "group": "market"
        }
    }
) }}

SELECT
    "MarketDate"::DATE AS date,
    "InfoCode"::TEXT AS info_code,
    "ExchIntCode"::TEXT AS exch_int_code,
    "InfoCode"::TEXT || '-' || "ExchIntCode"::TEXT AS external_quote_id,
    "ri" AS return_index,
    "sys_change_operation",
    "sys_change_version"::BIGINT AS sys_change_version,
    -- Type IDs from seeds (scalar subquery for optimizer)
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2PrimQtRI_Changes') }}
WHERE "MarketDate" >= '{{ var('min_data_date') }}'::date
