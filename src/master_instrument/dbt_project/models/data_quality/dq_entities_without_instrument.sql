{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_entities_without_instrument"],
            "group": "data_quality",
            "description": "Finds entities (companies) that have no associated instruments. This is an anomaly as companies should have at least one equity instrument."
        }
    }
) }}

-- Entities without any instrument
SELECT 
    e.entity_id,
    e.name AS entity_name,
    et.name AS entity_type
FROM {{ source('master', 'entity') }} e
JOIN {{ source('master', 'entity_type') }} et ON et.entity_type_id = e.entity_type_id
LEFT JOIN {{ source('master', 'instrument') }} i ON i.entity_id = e.entity_id
WHERE i.instrument_id IS NULL
