{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_currency"],
            "group": "reference"
        }
    }
) }}

-- Consolidate QA currency sources (RKD + DS2)

WITH rkd AS (
    SELECT DISTINCT 
        qrc."Desc_" AS code,
        d."Desc_" AS description,
        'RKD' AS source
    FROM {{ source('raw', 'qa_RKDFndCode') }} qrc
    JOIN {{ source('raw', 'qa_RKDFndDesc') }} d 
        ON d."Code" = qrc."Desc_" 
        AND d."Type_" = qrc."Type_"
    WHERE qrc."Type_" = '58'
),

ds2 AS (
    SELECT 
        dxrf."Code" AS code,
        dxrf."Desc_" AS description,
        'DS2' AS source
    FROM {{ source('raw', 'qa_DS2XRef') }} dxrf
    WHERE dxrf."Type_" = 3
)

-- Union all QA sources
SELECT * FROM rkd
UNION ALL
SELECT * FROM ds2
