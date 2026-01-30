{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_companies_without_weblink"],
            "group": "data_quality",
            "description": "Identifies companies that have no weblinks (website, investor relations URLs). Useful for data enrichment prioritization."
        }
    }
) }}

-- Companies without any weblinks
SELECT 
    c.company_id,
    e.name AS company_name
FROM {{ source('master', 'company') }} c
JOIN {{ source('master', 'entity') }} e ON e.entity_id = c.company_id
LEFT JOIN {{ source('master', 'company_weblink') }} cw ON cw.company_id = c.company_id
WHERE cw.company_weblink_id IS NULL
