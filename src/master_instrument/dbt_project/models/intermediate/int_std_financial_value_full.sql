{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","fundamental","int_std_financial_value_full"],
            "group": "fundamental"
        }
    }
) }}


SELECT  s.company_id ,
    sfs.std_financial_statement_id,
    fv.item AS std_financial_item_id,
    {{ convert_financial_value_units('value', 'units_conv_to_code', 'item_precision') }} AS converted_value,
    {{ get_conversion_factor('value', 'units_conv_to_code', 'item_precision') }} AS conversion_factor,
    fv.value,
    s.period_end_date,
    s.filing_end_date,
    fv.code,
    sfs.statement_type_id,
    s.period_type_id,
    fv.units_conv_to_code AS converted_unit,
    fv.curr_conv_to_code AS converted_currency,
    fv.item_precision,
    s.calendar_end_date
FROM {{ ref('stg_qa_std_financial_value_full') }} fv
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em
    ON em.external_entity_id = fv.code
    AND em.data_source_id = fv.data_source_id
    AND em.entity_type_id = fv.entity_type_id
JOIN master.std_financial_filing s
    ON s.company_id = em.internal_entity_id
    AND s.period_end_date = fv.per_end_dt
    AND s.filing_end_date = fv.stmt_dt
    AND s.period_type_id = fv.per_type_code
JOIN master.std_financial_statement sfs 
    ON sfs.std_financial_filing_id = s.std_financial_filing_id 
    AND sfs.statement_type_id = fv.stmt_type_code
-- Note: Item filtering already done in stg_qa_std_financial_value_full via stg_qa_std_financial_item join
