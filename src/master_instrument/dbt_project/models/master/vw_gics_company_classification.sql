{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_gics_company_classification"],
            "group": "reference"
        }
    }
) }}

-- GICS Company Classification: Flattened GICS hierarchy for each company
-- Uses recursive CTE to traverse the classification tree from mapped node up to root

WITH RECURSIVE gics_scheme AS (
    -- Get GICS scheme ID
    SELECT classification_scheme_id 
    FROM {{ source('master', 'classification_scheme') }}
    WHERE mnemonic = 'GICS'
),
hierarchy AS (
    -- Starting point: the node mapped to the entity
    SELECT 
        ec.entity_id,
        cn.classification_scheme_id,
        cn.code,
        cn.name,
        cn.level_number,
        cn.parent_code
    FROM {{ source('master', 'entity_classification') }} ec
    JOIN {{ source('master', 'classification_node') }} cn 
        ON cn.classification_scheme_id = ec.classification_scheme_id
        AND cn.code = ec.classification_node_code
    JOIN gics_scheme gs 
        ON gs.classification_scheme_id = ec.classification_scheme_id
    
    UNION ALL
    
    -- Traverse up the tree via parent_code
    SELECT 
        h.entity_id,
        cn.classification_scheme_id,
        cn.code,
        cn.name,
        cn.level_number,
        cn.parent_code
    FROM hierarchy h
    JOIN {{ source('master', 'classification_node') }} cn 
        ON cn.classification_scheme_id = h.classification_scheme_id
        AND cn.code = h.parent_code
    WHERE h.parent_code IS NOT NULL
)
SELECT 
    e.entity_id,
    e.name AS entity_name,
    MAX(CASE WHEN h.level_number = 1 THEN h.code END) AS sector_code,
    MAX(CASE WHEN h.level_number = 1 THEN h.name END) AS sector_name,
    MAX(CASE WHEN h.level_number = 2 THEN h.code END) AS industry_group_code,
    MAX(CASE WHEN h.level_number = 2 THEN h.name END) AS industry_group_name,
    MAX(CASE WHEN h.level_number = 3 THEN h.code END) AS industry_code,
    MAX(CASE WHEN h.level_number = 3 THEN h.name END) AS industry_name,
    MAX(CASE WHEN h.level_number = 4 THEN h.code END) AS sub_industry_code,
    MAX(CASE WHEN h.level_number = 4 THEN h.name END) AS sub_industry_name
FROM hierarchy h
JOIN {{ source('master', 'entity') }} e 
    ON e.entity_id = h.entity_id
GROUP BY e.entity_id, e.name
