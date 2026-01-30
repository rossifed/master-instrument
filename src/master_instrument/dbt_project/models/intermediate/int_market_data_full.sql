{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_market_data_full"],
            "group": "market"
        }
    }
) }}

SELECT
    smd.date AS trade_date,
    qm.internal_quote_id AS quote_id,
    ccy.currency_id,
    smd.open,
    smd.high,
    smd.low,
    smd.close,
    smd.volume,
    smd.bid,
    smd.ask,
    smd.vwap
FROM {{ ref('stg_qa_market_data_full') }} smd
JOIN master.quote_mapping qm
    ON qm.external_quote_id = smd.external_quote_id
    AND qm.data_source_id = smd.data_source_id
JOIN master.currency ccy
    ON ccy.code = smd.iso_curr_code
