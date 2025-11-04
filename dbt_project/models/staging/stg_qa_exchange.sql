{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_exchange"]
        }
    }
) }}

SELECT DISTINCT
    sqq.exch_int_code::TEXT  AS external_exchange_id,
    sqq.exch_int_code        AS exch_int_code,
    sqq.ds_exch_code,
    sqq.exch_type,
    sqq.exch_name,
    sqq.exch_mnem,
    sqq.exch_ctry_code,
    sqq.ctry_code_type,
    'QA'                     AS source
FROM {{ ref('stg_qa_quote') }} AS sqq
