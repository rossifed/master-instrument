{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_statements_without_values"],
            "group": "data_quality",
            "description": "Identifies financial statements that have no value records. May indicate incomplete data loading or empty filings."
        }
    }
) }}

-- Statements without any values
SELECT 
    fs.std_financial_statement_id,
    fs.std_financial_filing_id,
    fst.name AS statement_type
FROM {{ source('master', 'std_financial_statement') }} fs
JOIN {{ source('master', 'financial_statement_type') }} fst 
    ON fst.financial_statement_type_id = fs.statement_type_id
LEFT JOIN {{ source('master', 'std_financial_value') }} fv 
    ON fv.std_financial_statement_id = fs.std_financial_statement_id
WHERE fv.std_financial_statement_id IS NULL
