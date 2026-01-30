{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_dividend"],
            "group": "reference"
        }
    }
) }}

SELECT 
    d."InfoCode"::TEXT AS external_instrument_id,
    d."EventNum" AS event_num,
    TRIM(d."DivTypeCode") AS div_type_code,
    d."DivRate" AS div_rate,
    d."PayDate" AS pay_date,
    (d."PayDateEstFlag" = 'E') AS is_pay_date_estimated,
    d."ISOCurrCode" AS iso_curr_code,
    d."EffectiveDate" AS effective_date,
    (d."EffDateEstFlag" = 'E') AS is_effective_date_estimated,
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2Div') }} d
WHERE d."DivRate" IS NOT NULL
  AND d."EffectiveDate" >= '{{ var('min_data_date') }}'::date
  AND EXISTS (
      SELECT 1 FROM {{ ref('stg_qa_security_mapping') }} sm
      WHERE sm.ds_info_code = d."InfoCode"
  )
