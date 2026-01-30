{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_corpact_type"],
            "group": "reference"
        }
    }
) }}

SELECT
  TRIM("Code") AS code,
  "Desc_" AS description
FROM {{ source('raw', 'qa_DS2XRef') }}
WHERE "Type_" = 6
