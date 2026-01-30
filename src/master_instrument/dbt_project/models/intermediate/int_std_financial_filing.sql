{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","fundamental","int_std_financial_filing"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    em.internal_entity_id AS company_id,
    fpt.financial_period_type_id AS period_type_id,
    f.per_end_dt AS period_end_date,
    f.stmt_dt AS filing_end_date,
    f.cal_stmt_dt AS calendar_end_date,
    f.fyr AS fiscal_year,
    f.fiscal_mth AS fiscal_month,
    (f.per_type_code <> 1) AS is_interim,
    f.is_hybrid,
    f.final_filing AS is_final,
    f.orig_annc_dt AS announcement_date,
    rep_ccy.currency_id AS reported_currency_id,
    conv_ccy.currency_id AS converted_currency_id,
    f.units_rep_code AS reported_unit,
    f.units_conv_to_code AS converted_unit,
    f.data_source_id
FROM {{ ref('stg_qa_std_financial_filing') }} f
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em  
    ON em.external_entity_id = f.code
    AND em.data_source_id = f.data_source_id
    AND em.entity_type_id = f.entity_type_id
JOIN master.financial_period_type fpt 
    ON fpt.financial_period_type_id = f.per_type_code_internal
LEFT JOIN master.currency rep_ccy 
    ON rep_ccy.code = f.curr_rep_code
LEFT JOIN master.currency conv_ccy 
    ON conv_ccy.code = f.curr_conv_to_code
