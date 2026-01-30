{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_quote"],
            "group": "reference"
        }
    }
) }}

SELECT
    qm.internal_quote_id,
    sqq.external_quote_id,
    im.internal_instrument_id AS instrument_id,
    vm.internal_venue_id AS venue_id,
    sqq.ric,
    sqq.ticker,
    CASE
        WHEN sqq.price_unit LIKE 'E%' THEN
            POWER(10, -CAST(SUBSTRING(sqq.price_unit FROM 3 FOR 3) AS INT))
        ELSE 1
    END AS price_unit,
    sqq.mic,
    sqq.mic_desc AS market_name,
    ccy.currency_id,
    sqq.is_primary_exchange AS is_primary,
    sqq.data_source_id
FROM {{ ref('stg_qa_quote') }} sqq 
-- INNER JOIN for required mappings
JOIN master.instrument_mapping im
    ON im.external_instrument_id = sqq.external_instrument_id 
    AND im.data_source_id = sqq.data_source_id
    AND im.instrument_type_id = sqq.instrument_type_id
JOIN master.venue_mapping vm
    ON vm.external_venue_id = sqq.external_venue_id
    AND vm.data_source_id = sqq.data_source_id
    AND vm.venue_type_id = sqq.venue_type_id
LEFT JOIN master.quote_mapping qm 
    ON qm.external_quote_id = sqq.external_quote_id 
    AND qm.data_source_id = sqq.data_source_id
JOIN master.currency ccy 
    ON ccy.code = sqq.iso_curr_code
