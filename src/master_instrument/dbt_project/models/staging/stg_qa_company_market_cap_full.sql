{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","market","stg_qa_company_market_cap_full"],
            "group": "market"
        }
    }
) }}

SELECT
    mv."ValDate"::DATE AS val_date,
    sm.rkd_code,
    sm.rkd_code::TEXT AS external_entity_id,
    mv."ConsolMktVal" AS consol_mkt_val,
    mv."ConsolNumShrs" AS consol_num_shrs,
    cq."PrimISOCurrCode" AS iso_currency_code,
    sm.ds_ctry_is_prim_qt,  -- For deduplication priority
    sm.rkd_issue_id,        -- For deduplication priority
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2MktVal') }} mv
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} cq
    ON cq."InfoCode" = mv."InfoCode"
JOIN {{ ref('stg_qa_security_mapping') }} sm
    ON sm.ds_info_code = mv."InfoCode"
WHERE mv."ValDate" >= '{{ var('min_data_date') }}'::date
  AND mv."InfoCode" IS NOT NULL
