{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_equity_type"]
        }
    }
) }}

SELECT dxrf."Code" AS mnemonic,
       dxrf."Desc_" AS description,
       'QA' AS source
FROM {{ source('raw', 'qa_DS2XRef') }} dxrf
WHERE "Type_" = 2
