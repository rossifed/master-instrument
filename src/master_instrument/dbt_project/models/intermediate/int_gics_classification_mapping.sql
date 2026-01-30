{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_gics_classification_mapping"],
            "group": "reference"
        }
    }
) }}

WITH gics_raw AS (
    SELECT
        eq.external_company_id,
        eq.data_source_id,
        eq.entity_type_id,
        eq.isin,
        eq.is_primary_country,
        'GICS' AS scheme_code,
        ssgc.gics_sub_industry_code AS classification_node_code
    FROM {{ ref('stg_s3_gics_classification') }} ssgc
    JOIN {{ ref('stg_qa_equity') }} eq
      ON eq.isin = ssgc.isin
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY external_company_id, scheme_code
            ORDER BY is_primary_country DESC, isin
        ) AS rn
    FROM gics_raw
)

SELECT DISTINCT
    external_company_id,
    data_source_id,
    entity_type_id,
    scheme_code,
    classification_node_code
FROM ranked
WHERE rn = 1
