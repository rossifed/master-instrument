{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_company_weblink"],
            "group": "reference"
        }
    }
) }}

SELECT
    em.internal_entity_id AS company_id,
    sqcw.url,
    sqcw.weblink_type_id,
    sqcw.last_updated
FROM {{ ref('stg_qa_company_weblink') }} sqcw
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em
    ON em.external_entity_id = sqcw.external_company_id
    AND em.data_source_id = sqcw.data_source_id
    AND em.entity_type_id = sqcw.entity_type_id
