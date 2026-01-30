{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","fundamental","int_std_financial_statement"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    sff.std_financial_filing_id,
    fst.financial_statement_type_id AS statement_type_id,
    sfs.is_complete,
    sfs.is_consolidated,
    sfs.source_dt AS public_date,
    sfs.stmt_last_upd_dt AS last_update_date,
    sfs.source,
    sfs.data_source_id
FROM {{ ref('stg_qa_std_financial_statement') }} sfs
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em  
    ON em.external_entity_id = sfs.code
    AND em.data_source_id = sfs.data_source_id
    AND em.entity_type_id = sfs.entity_type_id
JOIN master.financial_statement_type fst 
    ON fst.financial_statement_type_id = sfs.stmt_type_code
JOIN master.std_financial_filing sff 
    ON sff.company_id = em.internal_entity_id
    AND sff.period_end_date = sfs.per_end_dt
    AND sff.filing_end_date = sfs.stmt_dt
    AND sff.period_type_id = sfs.per_type_code_internal
