{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_entity_classification"],
            "group": "reference"
        }
    }
) }}

SELECT
    em.internal_entity_id AS entity_id,
    cs.classification_scheme_id,
    gcm.classification_node_code
FROM {{ ref('int_gics_classification_mapping') }} gcm
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em 
    ON em.external_entity_id = gcm.external_company_id
    AND em.data_source_id = gcm.data_source_id
    AND em.entity_type_id = gcm.entity_type_id
JOIN master.classification_scheme cs 
    ON cs.mnemonic = gcm.scheme_code
