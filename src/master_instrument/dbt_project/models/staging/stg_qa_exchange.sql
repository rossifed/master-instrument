{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_exchange"],
            "group": "reference"
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
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT venue_type_id FROM {{ ref('venue_type') }} WHERE mnemonic = 'EXCH') AS venue_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ ref('stg_qa_quote') }} AS sqq
