{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_corpact_event"],
            "group": "corporate_actions"
        }
    }
) }}

-- Corporate Action Event View: Corporate action events with instrument and company details

SELECT 
    -- Event ID
    ce.corpact_event_id,
    
    -- Instrument
    i.instrument_id,
    i.name AS instrument_name,
    i.symbol,
    
    -- Company
    en.entity_id AS company_id,
    en.name AS company_name,
    
    -- Equity identifiers
    eq.isin,
    eq.sedol,
    eq.cusip,
    eq.ric,
    eq.ticker,
    eq.delisted_date,
    
    -- Currency
    cc.currency_id,
    cc.code AS currency_code,
    
    -- Corporate Action Type
    ct.corpact_type_id,
    ct.code AS corpact_type_code,
    ct.description AS corpact_type_description,
    
    -- Event details
    ce.event_sequence,
    ce.announced_date,
    ce.effective_date,
    ce.record_date,
    ce.expiry_date,
    ce.cash_amount,
    ce.old_shares_count,
    ce.new_shares_count
FROM {{ source('master', 'corpact_event') }} ce
JOIN {{ source('master', 'instrument') }} i 
    ON i.instrument_id = ce.equity_id
JOIN {{ source('master', 'equity') }} eq 
    ON eq.equity_id = i.instrument_id
JOIN {{ source('master', 'entity') }} en 
    ON en.entity_id = i.entity_id
JOIN {{ source('master', 'corpact_type') }} ct 
    ON ct.corpact_type_id = ce.corpact_type_id
JOIN {{ source('master', 'currency') }} cc 
    ON cc.currency_id = ce.currency_id
