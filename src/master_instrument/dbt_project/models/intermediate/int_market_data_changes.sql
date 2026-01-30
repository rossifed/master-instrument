{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_market_data_changes"],
            "group": "market"
        }
    }
) }}

{{ cdc_deduplicate(
    source_ref="stg_qa_market_data_changes",
    load_table="market_data_load",
    unique_keys=["info_code", "exch_int_code", "date"]
) }}

SELECT
    dc.date AS trade_date,
    qm.internal_quote_id AS quote_id,
    ccy.currency_id,
    dc.open,
    dc.high,
    dc.low,
    dc.close,
    dc.volume,
    dc.bid,
    dc.ask,
    dc.vwap,
    dc.sys_change_operation,
    dc.sys_change_version,
    dc.last_loaded_version
FROM deduplicated_changes dc
JOIN master.quote_mapping qm
    ON qm.external_quote_id = dc.external_quote_id
    AND qm.data_source_id = dc.data_source_id
JOIN master.currency ccy
    ON ccy.code = dc.iso_curr_code
