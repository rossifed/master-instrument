{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_company_weblink"],
            "group": "reference"

        }
    }
) }}

SELECT
    qrcwl."Code"::TEXT AS external_company_id,
    qrcwl.url,
    wt.weblink_type_id,
    qrcwl."LastUpdDt" AS last_updated,
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_RKDFndCmpWebLink') }} AS qrcwl
JOIN {{ ref('stg_qa_weblink_type') }} AS wt
    ON wt.weblink_type_id = qrcwl."URLTypeCode"
WHERE qrcwl.url IS NOT NULL
