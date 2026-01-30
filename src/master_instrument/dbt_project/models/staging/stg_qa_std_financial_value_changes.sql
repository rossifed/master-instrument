{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_value_changes"],
            "group": "fundamental"
        }
    }
) }}

SELECT
    qrspf."Code"::TEXT AS code,
    qrspf."PerEndDt" AS per_end_dt,
    qrspf."StmtDt" AS stmt_dt,
    qrsfvc."StmtTypeCode" AS stmt_type_code,
    qrsfvc."Item" AS item,
    qrsfvc."Value_" AS value,
    sqsfi.item_precision,
    fptm.internal_period_type_id AS per_type_code,
    convccy."Desc_" AS curr_conv_to_code,
    qrspf."UnitsConvToCode" AS units_conv_to_code,
    qrspf."CalPerEndDt" AS cal_per_end_dt,
    qrspf."CalStmtDt" AS cal_stmt_dt,
    -- Type IDs from seeds (scalar subqueries for better plan)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id,
    qrsfvc."sys_change_operation" AS sys_change_operation,
    qrsfvc."sys_change_version" AS sys_change_version
FROM {{ source('raw', 'qa_RKDFndStdFinVal_Changes') }} qrsfvc
JOIN {{ source('raw', 'qa_RKDFndStdPerFiling') }} qrspf
    ON qrspf."Code" = qrsfvc."Code"
   AND qrspf."PerTypeCode" = qrsfvc."PerTypeCode"
   AND qrspf."PerEndDt" = qrsfvc."PerEndDt"
   AND qrspf."StmtDt" = qrsfvc."StmtDt"
JOIN {{ ref('stg_qa_std_financial_item') }} sqsfi
    ON sqsfi.item = qrsfvc."Item"
JOIN {{ source('raw', 'qa_RKDFndCode') }} convccy 
    ON convccy."Code" = qrspf."CurrConvToCode" 
    AND convccy."Type_" = 58
JOIN {{ ref('financial_period_type_mapping') }} fptm
    ON fptm.external_period_type_id = qrsfvc."PerTypeCode"
   AND fptm.source = 'QA'
WHERE qrspf."PerEndDt" >= '{{ var('min_data_date') }}'::date
