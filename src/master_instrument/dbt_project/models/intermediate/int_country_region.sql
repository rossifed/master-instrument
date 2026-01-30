{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_country_region"],
            "group": "reference"
        }
    }
) }}

SELECT 
    c.country_id, 
    r.region_id
FROM {{ ref('stg_s3_country_region') }} sscr
JOIN master.country c 
    ON c.code_alpha2 = sscr.country_code
JOIN master.region r 
    ON r.code = sscr.region_code
