{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_company_weblink"]
        }
    }
) }}

SELECT
    qrcwl."Code"::TEXT AS external_company_id,
    qrcwl.url,
    wt.code AS weblink_type_id,
    qrcwl."LastUpdDt" AS last_updated
FROM {{ source('raw', 'qa_RKDFndCmpWebLink') }} AS qrcwl
JOIN {{ ref('stg_qa_weblink_type') }} AS wt
    ON wt.code = qrcwl."URLTypeCode"
WHERE qrcwl.url IS NOT NULL
