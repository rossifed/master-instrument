{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_dividend_type"],
            "group": "reference"
        }
    }
) }}

SELECT 
    TRIM(qdx."Code") AS code, 
    qdx."Desc_" AS description
FROM {{ source('raw', 'qa_DS2XRef') }} qdx
WHERE qdx."Type_" = 8
