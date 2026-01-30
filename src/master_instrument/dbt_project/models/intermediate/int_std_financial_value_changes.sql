{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","fundamental","int_std_financial_value_changes"],
            "group": "fundamental"
        }
    }
) }}

{{ cdc_deduplicate(
    source_ref="stg_qa_std_financial_value_changes",
    load_table="std_financial_value_load",
    unique_keys=["code", "per_end_dt", "per_type_code", "stmt_dt", "stmt_type_code", "item"]
) }}

SELECT
    em.internal_entity_id AS company_id,
    sfs.std_financial_statement_id,
    fv.item AS std_financial_item_id,
    {{ convert_financial_value_units('value', 'units_conv_to_code', 'item_precision') }} AS converted_value,
    {{ get_conversion_factor('value', 'units_conv_to_code', 'item_precision') }} AS conversion_factor,
    fv.value,
    fv.per_end_dt AS period_end_date,
    fv.stmt_dt AS filing_end_date,
    fv.code,
    fv.stmt_type_code AS statement_type_id,
    fv.per_type_code AS period_type_id,
    fv.units_conv_to_code AS converted_unit,
    fv.curr_conv_to_code AS converted_currency,
    fv.item_precision,
    fv.cal_per_end_dt AS calendar_end_date,
    fv.sys_change_operation,
    fv.sys_change_version,
    fv.last_loaded_version
FROM deduplicated_changes fv
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
-- Filter to only include items that exist in master
JOIN master.std_financial_item sfi
    ON sfi.std_financial_item_id = fv.item