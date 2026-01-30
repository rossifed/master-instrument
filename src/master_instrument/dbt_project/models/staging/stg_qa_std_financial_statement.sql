{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_statement"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    s."Code"::TEXT AS code,
    s."PerEndDt" AS per_end_dt,
    s."PerTypeCode" AS per_type_code,
    s."StmtDt" AS stmt_dt,
    s."StmtTypeCode" AS stmt_type_code,
    (s."CompStmtCode" = 1) AS is_complete,
    (s."Consolidated" = 1) AS is_consolidated,
    s."SourceDt" AS source_dt,
    s."StmtLastUpdDt" AS stmt_last_upd_dt,
    s."Source" AS source,
    fptm.internal_period_type_id AS per_type_code_internal,
    -- Type IDs from seeds (scalar subqueries to avoid cross join)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_RKDFndStdStmt') }} s
JOIN {{ ref('financial_period_type_mapping') }} fptm 
    ON fptm.external_period_type_id = s."PerTypeCode"
    AND fptm.source = 'QA'
WHERE s."PerEndDt" >= '{{ var('min_data_date') }}'::date
