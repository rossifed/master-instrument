{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_company_market_cap_changes"],
            "group": "market"
        }
    }
) }}

{{ cdc_deduplicate(
    source_ref="stg_qa_company_market_cap_changes",
    load_table="company_market_cap_load",
    unique_keys=["external_entity_id", "val_date"]
) }}

SELECT
    dc.val_date AS valuation_date,
    em.internal_entity_id AS company_id,
    c.currency_id,
    (dc.consol_mkt_val * 1000000)::BIGINT AS market_cap,       -- source in millions
    (dc.consol_num_shrs * 1000)::BIGINT AS shares_outstanding, -- source in thousands
    dc.sys_change_operation,
    dc.sys_change_version,
    dc.last_loaded_version
FROM deduplicated_changes dc
-- INNER JOIN for required entity mapping
JOIN master.entity_mapping em
    ON em.external_entity_id = dc.external_entity_id
    AND em.data_source_id = dc.data_source_id
    AND em.entity_type_id = dc.entity_type_id
-- Map ISO currency code to currency_id
JOIN master.currency c
    ON c.code = dc.iso_currency_code
