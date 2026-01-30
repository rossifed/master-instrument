{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_corpact_event"],
            "group": "reference"
        }
    }
) }}

SELECT 
    im.internal_instrument_id      AS equity_id,
    stg.event_sequence,
    ct.corpact_type_id,
    res_im.internal_instrument_id  AS res_equity_id,
    stg.new_shares_count,
    stg.old_shares_count,
    cur.currency_id,
    stg.cash_amount,
    stg.offer_company_name,
    stg.announced_date,
    stg.record_date,
    stg.effective_date,
    stg.expiry_date
FROM {{ ref('stg_qa_corpact_event') }} stg
-- INNER JOIN for required instrument mapping
JOIN master.instrument_mapping im 
    ON im.external_instrument_id = stg.external_instrument_id
    AND im.data_source_id = stg.data_source_id
    AND im.instrument_type_id = stg.instrument_type_id
JOIN master.corpact_type ct
    ON ct.code = stg.corpact_type_code
LEFT JOIN master.instrument_mapping res_im 
    ON res_im.external_instrument_id = stg.res_external_instrument_id
    AND res_im.data_source_id = stg.data_source_id
    AND res_im.instrument_type_id = stg.instrument_type_id
LEFT JOIN master.currency cur
    ON cur.code = stg.currency_code
