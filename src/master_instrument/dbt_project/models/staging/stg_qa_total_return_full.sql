{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","market","stg_qa_total_return_full"],
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
    -- Type IDs from seeds (scalar subquery for optimizer)
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2PrimQtRI') }}
WHERE "MarketDate" >= '{{ var('min_data_date') }}'::date
