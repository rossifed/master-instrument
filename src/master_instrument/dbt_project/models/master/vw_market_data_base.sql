{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master",  "views","vw_market_data_base"],
            "group": "market"
        }
    }
) }}

-- Base view that enriches market_data with instrument, entity and currency info
-- All downstream market data views should use this as their source
SELECT 
    md.trade_date,
    md.quote_id,
    i.instrument_id,
    i.name AS instrument_name,
    e.entity_id,
    e.name AS entity_name,
    md.currency_id,
    c.code AS currency_code,
    q.is_primary,
    COALESCE(q.price_unit, 1.0) AS price_unit,
    md.open,
    md.high,
    md.low,
    md.close,
    md.volume,
    md.bid,
    md.ask,
    md.vwap,
    md.loaded_at
FROM {{ source('master', 'market_data') }} md
JOIN {{ source('master', 'quote') }} q
    ON q.quote_id = md.quote_id
JOIN {{ source('master', 'instrument') }} i
    ON i.instrument_id = q.instrument_id
JOIN {{ source('master', 'entity') }} e
    ON e.entity_id = i.entity_id
JOIN {{ source('master', 'currency') }} c
    ON c.currency_id = md.currency_id

