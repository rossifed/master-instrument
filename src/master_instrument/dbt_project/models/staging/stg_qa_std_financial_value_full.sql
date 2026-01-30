{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_value_full"],
            "group": "fundamental"
        }
    }
) }}
SELECT
    qrspf."Code"::TEXT AS code,
    qrspf."PerEndDt" AS per_end_dt,
    qrspf."StmtDt" AS stmt_dt,
    qrsfv."StmtTypeCode" AS stmt_type_code,
    sqsfi."Item" AS item,
    qrsfv."Value_" AS value,
    sqsfi."ItemPrecision" AS item_precision,
    fptm.internal_period_type_id AS per_type_code,
    convccy."Desc_" AS curr_conv_to_code,
    qrspf."UnitsConvToCode" AS units_conv_to_code,
    qrspf."CalPerEndDt" AS cal_per_end_dt,
    qrspf."CalStmtDt" AS cal_stmt_dt,
    -- Type IDs from seeds (scalar subqueries for better plan)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_RKDFndStdFinVal') }} qrsfv
JOIN {{ source('raw', 'qa_RKDFndStdPerFiling') }} qrspf
    ON qrspf."Code"        = qrsfv."Code"
   AND qrspf."PerTypeCode" = qrsfv."PerTypeCode"
   AND qrspf."PerEndDt"    = qrsfv."PerEndDt"
   AND qrspf."StmtDt"      = qrsfv."StmtDt"
JOIN {{ source('raw', 'qa_RKDFndStdItem') }} sqsfi
    ON sqsfi."Item" = qrsfv."Item" and sqsfi."Desc_" IS NOT NULL
  AND sqsfi."DataType" IN ('Float', 'Numeric')
JOIN {{ source('raw', 'qa_RKDFndCode') }} convccy 
    ON convccy."Code" = qrspf."CurrConvToCode" 
    AND convccy."Type_" = 58
JOIN {{ ref('financial_period_type_mapping') }} fptm
    ON fptm.external_period_type_id = qrsfv."PerTypeCode"
   AND fptm.source = 'QA'
WHERE qrspf."PerEndDt" >= '{{ var('min_data_date') }}'::date
