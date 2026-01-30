{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_filing"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    f."Code"::TEXT AS code,
    f."PerEndDt" AS per_end_dt,
    f."PerTypeCode" AS per_type_code,
    f."StmtDt" AS stmt_dt,
    f."CalStmtDt" AS cal_stmt_dt,
    (f."FinalFiling" = 1) AS final_filing,
    f."OrigAnncDt" AS orig_annc_dt,
    rep_ccy."Desc_" AS curr_rep_code,
    conv_ccy."Desc_" AS curr_conv_to_code,
    f."UnitsRepCode" AS units_rep_code,
    f."UnitsConvToCode" AS units_conv_to_code,
    (fsp."IsHybrid" = 1) AS is_hybrid,
    fsp."FiscalMth" AS fiscal_mth,
    fsp."Fyr" AS fyr,
    fptm.internal_period_type_id AS per_type_code_internal,
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_RKDFndStdPerFiling') }} f
LEFT JOIN {{ source('raw', 'qa_RKDFndStdPeriod') }} fsp
    ON fsp."Code" = f."Code" 
    AND fsp."PerEndDt" = f."PerEndDt" 
    AND (fsp."PerTypeCode" = 1) = (f."PerTypeCode" = 1)
JOIN {{ ref('financial_period_type_mapping') }} fptm 
    ON fptm.external_period_type_id = f."PerTypeCode"
    AND fptm.source = 'QA'
JOIN {{ source('raw', 'qa_RKDFndCode') }} rep_ccy 
    ON rep_ccy."Code" = f."CurrRepCode" 
    AND rep_ccy."Type_" = 58
JOIN {{ source('raw', 'qa_RKDFndCode') }} conv_ccy 
    ON conv_ccy."Code" = f."CurrConvToCode" 
    AND conv_ccy."Type_" = 58
WHERE f."PerEndDt" >= '{{ var('min_data_date') }}'::date 

