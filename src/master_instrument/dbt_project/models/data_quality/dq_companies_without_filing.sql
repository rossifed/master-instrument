{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_companies_without_filing"],
            "group": "data_quality",
            "description": "Identifies companies that have no financial filings. May indicate missing fundamental data or companies not covered by data provider."
        }
    }
) }}

-- Companies without any financial filing
SELECT 
    c.company_id,
    e.name AS company_name
FROM {{ source('master', 'company') }} c
JOIN {{ source('master', 'entity') }} e ON e.entity_id = c.company_id
LEFT JOIN {{ source('master', 'std_financial_filing') }} sff ON sff.company_id = c.company_id
WHERE sff.std_financial_filing_id IS NULL
