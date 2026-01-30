{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_classification_node"],
            "group": "reference"
        }
    }
) }}

-- Intermediate model to resolve classification node hierarchy
-- Uses recursive CTE to calculate level_number based on parent-child relationships
WITH RECURSIVE input_with_scheme AS (
    SELECT
        cs.classification_scheme_id,
        sn.code,
        sn.name,
        sn.parent_code
    FROM {{ ref('classification_node') }} sn
    JOIN master.classification_scheme cs 
      ON sn.scheme_mnemonic = cs.mnemonic
),
resolved_nodes AS (
    -- Level 1: Root nodes (no parent)
    SELECT
        code,
        name,
        parent_code,
        classification_scheme_id,
        1 AS level_number
    FROM input_with_scheme
    WHERE parent_code IS NULL
    
    UNION ALL
    
    -- Recursion: Child nodes
    SELECT
        c.code,
        c.name,
        c.parent_code,
        c.classification_scheme_id,
        p.level_number + 1
    FROM input_with_scheme c
    JOIN resolved_nodes p
        ON c.parent_code = p.code
        AND c.classification_scheme_id = p.classification_scheme_id
)
SELECT DISTINCT
    classification_scheme_id,
    code,
    name,
    parent_code,
    level_number
FROM resolved_nodes
ORDER BY classification_scheme_id, code
