{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_corpact_adjustment"],
            "group": "reference"
        }
    }
) }}


SELECT 
    adj."InfoCode"::TEXT      AS external_instrument_id, 
    adj."AdjType"       AS adj_type, 
    adj."AdjDate"       AS adj_date, 
    adj."EndAdjDate"    AS end_adj_date, 
    adj."AdjFactor"     AS adj_factor, 
    adj."CumAdjFactor"  AS cum_adj_factor,
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2Adj') }} adj
JOIN {{ ref('stg_qa_security_mapping') }} sqsm 
    ON sqsm.ds_info_code = adj."InfoCode"
WHERE adj."AdjDate" >= '{{ var('min_data_date') }}'::date
  AND adj."AdjFactor" IS NOT NULL
