{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_weblink_type"]
        }
    }
) }}

SELECT DISTINCT
    qrfc."Code" AS code,
    qrfc."Desc_" AS description,
    'QA' AS source
FROM {{ source('raw', 'qa_RKDFndCode') }} AS qrfc
WHERE qrfc."Type_" = 42
