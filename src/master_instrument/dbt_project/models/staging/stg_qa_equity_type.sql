{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_equity_type"],
            "group": "reference"
        }
    }
) }}

SELECT dxrf."Code" AS mnemonic,
       dxrf."Desc_" AS description,
       'QA' AS source
FROM {{ source('raw', 'qa_DS2XRef') }} dxrf
WHERE "Type_" = 2
