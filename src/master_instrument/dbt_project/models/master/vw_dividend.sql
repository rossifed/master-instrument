{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_dividend"],
            "group": "corporate_actions"
        }
    }
) }}

-- Dividend View: Dividend events with instrument and company details

SELECT 
    -- Dividend ID
    div.dividend_id,
    
    -- Instrument
    i.instrument_id,
    i.name AS instrument_name,
    
    -- Company
    en.entity_id AS company_id,
    en.name AS company_name,
    
    -- Equity identifiers
    eq.isin,
    eq.sedol,
    eq.ric,
    eq.ticker,
    eq.delisted_date,
    
    -- Currency
    cc.currency_id,
    cc.code AS currency_code,
    
    -- Dividend Type
    dt.dividend_type_id,
    dt.code AS dividend_type_code,
    dt.description AS dividend_type_description,
    
    -- Dividend details
    div.event_sequence,
    div.effective_date,
    div.pay_date,
    div.is_effective_date_estimated,
    div.is_pay_date_estimated
FROM {{ source('master', 'dividend') }} div
JOIN {{ source('master', 'instrument') }} i 
    ON i.instrument_id = div.equity_id
JOIN {{ source('master', 'equity') }} eq 
    ON eq.equity_id = i.instrument_id
JOIN {{ source('master', 'entity') }} en 
    ON en.entity_id = i.entity_id
JOIN {{ source('master', 'dividend_type') }} dt 
    ON dt.dividend_type_id = div.dividend_type_id
JOIN {{ source('master', 'currency') }} cc 
    ON cc.currency_id = div.currency_id
