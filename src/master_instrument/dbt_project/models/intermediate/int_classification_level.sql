{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_classification_level"],
            "group": "reference"
        }
    }
) }}
-- Intermediate model to enrich classification levels with scheme IDs
-- Joins seed classification levels with master classification schemes
SELECT
    cs.classification_scheme_id,
    sl.level_number,
    sl.name
FROM {{ ref('classification_level') }} sl
JOIN master.classification_scheme cs 
  ON sl.scheme_mnemonic = cs.mnemonic
ORDER BY cs.classification_scheme_id, sl.level_number
