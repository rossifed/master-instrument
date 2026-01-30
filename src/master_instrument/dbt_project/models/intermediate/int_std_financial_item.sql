{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","fundamental","int_std_financial_item"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    fi.item AS std_financial_item_id,
    fi.desc AS name,
    fst.financial_statement_type_id AS statement_type_id,
    fi.is_currency
FROM {{ ref('stg_qa_std_financial_item') }} fi
LEFT JOIN master.financial_statement_type fst 
    ON fst.financial_statement_type_id = fi.stmt_type_code
