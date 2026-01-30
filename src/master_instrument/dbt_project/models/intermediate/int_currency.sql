{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_currency"],
            "group": "reference"
        }
    }
) }}

-- Consolidate currencies from QA + Seed with deduplication
-- Priority: SEED > DS2 > RKD

WITH qa_currencies AS (
    SELECT * FROM {{ ref('stg_qa_currency') }}
),

seed_currencies AS (
    SELECT 
        code,
        name AS description,
        'SEED' AS source
    FROM {{ ref('currency') }}
),

all_currencies AS (
    SELECT * FROM qa_currencies
    UNION ALL
    SELECT * FROM seed_currencies
)

-- Deduplicate: keep highest priority source per code
-- Filter out rows with NULL name to avoid NOT NULL constraint violations
SELECT DISTINCT ON (code)
    code,
    description as name 
FROM all_currencies
WHERE description IS NOT NULL  -- Exclude NULL names
ORDER BY 
    code,
    CASE source
        WHEN 'SEED' THEN 1  -- Highest priority
        WHEN 'DS2' THEN 2
        WHEN 'RKD' THEN 3   -- Lowest priority
    END
