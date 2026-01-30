{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_equity_listing"],
            "group": "reference"
        }
    }
) }}

SELECT  
    q.quote_id,
    e.instrument_id,
    e.company_id,
    e.name,
    e.symbol,
    e.delisted_date,
    e.is_major_security,
    e.security_id,
    e.isin,
    e.sedol,
    e.cusip,
    e.description,
    q.is_primary,
    q.market_name,
    q.mic,
    q.ric,
    q.ticker,
    q.price_unit,
    c.code AS currency,
    v.mnemonic AS venue_mnemonic,
    vt.mnemonic AS venue_type,
    vt.name AS venue_type_description
FROM {{ source('master', 'quote') }} q
JOIN {{ source('master', 'venue') }} v 
    ON v.venue_id = q.venue_id
LEFT JOIN {{ source('master', 'currency') }} c 
    ON c.currency_id = q.currency_id
JOIN {{ source('master', 'venue_type') }} vt 
    ON vt.venue_type_id = v.venue_type_id
JOIN {{ ref('vw_equity') }} e
    ON e.instrument_id = q.instrument_id
