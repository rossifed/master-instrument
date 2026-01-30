{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_weblink_type"],
            "group": "reference"
        }
    }
) }}

SELECT DISTINCT
    qrfc."Code" AS weblink_type_id,
    qrfc."Desc_" AS description,
    'QA' AS source
FROM {{ source('raw', 'qa_RKDFndCode') }} AS qrfc
WHERE qrfc."Type_" = 42
