{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","s3","reference","stg_s3_country_region"],
            "group": "reference"
        }
    }
) }}

WITH region_flags AS (
    SELECT *
    FROM {{ source('raw', 's3_regions_master') }}
),
countries AS (
    SELECT *
    FROM {{ ref('country') }}
),
regions AS (
    SELECT *
    FROM {{ ref('region') }}
),
unpivoted AS (
    SELECT iso2_code, 'Developed' AS region_name, Developed AS flag FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Emerging', Emerging FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Frontier', Frontier FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Americas', Americas FROM region_flags
    UNION ALL
    SELECT iso2_code, 'EMEA', EMEA FROM region_flags
    UNION ALL
    SELECT iso2_code, 'APAC', APAC FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Standalone', Standalone FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Euro Zone', Eurozone FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Europe', Europe FROM region_flags
    UNION ALL
    SELECT iso2_code, 'MENA', MENA FROM region_flags
    UNION ALL
    SELECT iso2_code, 'BRICS', BRICS FROM region_flags
    UNION ALL
    SELECT iso2_code, 'Latin America', Latinamerica FROM region_flags
),
active_flags AS (
    SELECT iso2_code, region_name
    FROM unpivoted
    WHERE flag = 1
)
SELECT
    c.name AS country_name,
    c.code_alpha2 AS country_code,
    r.name AS region_name,
    r.code AS region_code
FROM active_flags f
JOIN countries c ON f.iso2_code = c.code_alpha2
JOIN regions r ON LOWER(f.region_name) = LOWER(r.name)
