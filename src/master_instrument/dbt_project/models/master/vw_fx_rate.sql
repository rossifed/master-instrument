{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_fx_rate"],
            "group": "market"
        }
    }
) }}

-- FX Rate View: Currency exchange rates with base and quote currency details

SELECT 
    -- Date
    fr.rate_date,
    
    -- Base Currency
    fr.base_currency_id,
    bc.code AS base_currency_code,
    bc.name AS base_currency_name,
    
    -- Quote Currency
    fr.quote_currency_id,
    qc.code AS quote_currency_code,
    qc.name AS quote_currency_name,
    
    -- Rates
    fr.ask_rate,
    fr.bid_rate,
    fr.mid_rate,
    
    -- Metadata
    fr.loaded_at
FROM {{ source('master', 'fx_rate') }} fr
JOIN {{ source('master', 'currency') }} bc 
    ON bc.currency_id = fr.base_currency_id
JOIN {{ source('master', 'currency') }} qc 
    ON qc.currency_id = fr.quote_currency_id
