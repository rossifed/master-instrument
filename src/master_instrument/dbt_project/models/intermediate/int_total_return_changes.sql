{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_total_return_changes"],
            "group": "market"
        }
    }
) }}

{{ cdc_deduplicate(
    source_ref="stg_qa_total_return_changes",
    load_table="total_return_load",
    unique_keys=["info_code", "exch_int_code", "date"]
) }}

SELECT
    dc.date AS value_date,
    qm.internal_quote_id AS quote_id,
    dc.return_index AS value,
    dc.sys_change_operation,
    dc.sys_change_version,
    dc.last_loaded_version
FROM deduplicated_changes dc
JOIN master.quote_mapping qm
    ON qm.external_quote_id = dc.external_quote_id
    AND qm.data_source_id = dc.data_source_id
