{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_last_market_data"],
            "group": "market"
        }
    }
) }}

-- Last Market Data: Most recent market data for each quote
-- Optimized for TimescaleDB with chunk pruning via date filter
-- Includes USD conversion for consistent comparison
-- Note: Only returns quotes with market data in the last 10 days

WITH latest_market_data AS (
    SELECT DISTINCT ON (quote_id)
        quote_id,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        bid,
        ask,
        vwap,
        currency_id,
        loaded_at
    FROM {{ source('master', 'market_data') }}
    WHERE close IS NOT NULL
      AND trade_date > (CURRENT_DATE - INTERVAL '10 days')
    ORDER BY quote_id, trade_date DESC
)

SELECT
    -- Instrument
    i.instrument_id,
    i.name AS instrument_name,

    -- Company
    e.entity_id AS company_id,
    e.name AS company_name,

    -- Equity identifiers
    eq.isin,
    eq.sedol,
    eq.ric,
    eq.ticker,

    -- Quote
    q.quote_id,
    q.mic,
    q.is_primary,

    -- Market Data (latest)
    lmd.trade_date,
    lmd.open,
    lmd.high,
    lmd.low,
    lmd.close,
    lmd.volume,
    lmd.bid,
    lmd.ask,
    lmd.vwap,

    -- Currency
    lmd.currency_id,
    ccy.code AS currency_code,

    -- USD conversion
    fx.mid_rate AS fx_rate_to_usd,
    lmd.close * fx.mid_rate AS close_usd,

    -- Metadata
    lmd.loaded_at
FROM latest_market_data lmd
JOIN {{ source('master', 'quote') }} q
    ON q.quote_id = lmd.quote_id
JOIN {{ source('master', 'instrument') }} i
    ON i.instrument_id = q.instrument_id
JOIN {{ source('master', 'entity') }} e
    ON e.entity_id = i.entity_id
JOIN {{ source('master', 'equity') }} eq
    ON eq.equity_id = i.instrument_id
JOIN {{ source('master', 'currency') }} ccy
    ON ccy.currency_id = lmd.currency_id
JOIN {{ source('master', 'currency') }} usd
    ON usd.code = 'USD'
JOIN {{ source('master', 'fx_rate') }} fx
    ON fx.base_currency_id = lmd.currency_id
    AND fx.quote_currency_id = usd.currency_id
    AND fx.rate_date = lmd.trade_date
    AND fx.rate_date > (CURRENT_DATE - INTERVAL '10 days')
