{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","s3","reference","stg_s3_gics_classification"],
            "group": "reference"
        }
    }
) }}

SELECT
    ssgc.isin,
    ssgc.gics_sub_industry_code::TEXT AS gics_sub_industry_code
FROM {{ source('raw', 's3_gics_classification') }} ssgc
WHERE ssgc.isin IS NOT NULL
  AND TRIM(ssgc.isin) <> ''
  AND ssgc.gics_sub_industry_code IS NOT NULL
