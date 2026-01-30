{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","s3","reference","stg_s3_atonra_theme"],
            "group": "reference"
        }
    }
) }}


SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_software') }} 
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM  {{ source('raw', 's3_theme_biotech') }} 
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_artificial_intelligence') }} 
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_sustainable_future') }} 
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM  {{ source('raw', 's3_theme_cybersecurity') }}
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_securityspace') }}
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_mobilepayments') }}
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_blockchain') }}
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_fintech') }} 
WHERE isin IS NOT NULL

UNION ALL

SELECT
    theme AS theme,
    Level_1 AS level_1,
    Level_2 AS level_2,
    Level_3 AS level_3,
    isin AS isin
FROM {{ source('raw', 's3_theme_bionics') }} 
WHERE isin IS NOT NULL
