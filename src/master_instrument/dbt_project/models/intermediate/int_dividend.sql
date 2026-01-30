{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_dividend"],
            "group": "reference"
        }
    }
) }}

SELECT 
    im.internal_instrument_id AS equity_id,
    d.event_num AS event_sequence,
    dt.dividend_type_id,
    d.div_rate AS dividend_rate,
    d.pay_date,
    d.is_pay_date_estimated,
    c.currency_id,
    d.effective_date,
    d.is_effective_date_estimated
FROM {{ ref('stg_qa_dividend') }} d
-- INNER JOIN for required instrument mapping
JOIN master.instrument_mapping im 
    ON im.external_instrument_id = d.external_instrument_id
    AND im.data_source_id = d.data_source_id
    AND im.instrument_type_id = d.instrument_type_id
JOIN master.dividend_type dt 
    ON dt.code = d.div_type_code
JOIN master.currency c 
    ON c.code = d.iso_curr_code
