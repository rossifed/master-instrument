{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_period"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    p."Code" AS code,
    p."Code"::TEXT AS external_company_id,
    p."PerEndDt" AS period_end_date, 
    p."PerTypeCode" AS period_type_code,
    p."CalPerEndDt" AS calendar_end_date,
    p."Fyr" AS fiscal_year,
    p."FiscalMth" AS fiscal_month, 
    p."InterimNo" AS interim_number,
    (p."IsHybrid" = 1) AS is_hybrid,
    (p."InterimNo" IS NOT NULL) AS is_interim,
    'QA' AS source
FROM {{ source('raw', 'qa_RKDFndStdPeriod') }} p
WHERE p."PerEndDt" >= '{{ var('min_data_date') }}'::date
