{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_currency"]
        }
    }
) }}

SELECT dxrf."Code" AS code,
       dxrf."Desc_" AS description,
       'QA' AS source
FROM {{ source('raw', 'qa_DS2XRef') }} dxrf
WHERE "Type_" = 3
