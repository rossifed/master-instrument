{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master","views", "vw_last_company_market_cap"],
            "group": "market"
        }
    }
) }}

-- Last Company Market Cap: Most recent consolidated market cap per company
-- Includes USD conversion for consistent ranking
-- Optimized for TimescaleDB with chunk pruning via date filter
-- Note: Only returns companies with market cap data in the last 10 days

WITH latest_market_cap AS (
    SELECT DISTINCT ON (company_id)
        company_id,
        valuation_date,
        shares_outstanding,
        market_cap,
        currency_id,
        loaded_at
    FROM {{ source('master', 'company_market_cap') }}
    WHERE market_cap IS NOT NULL
      AND valuation_date > (CURRENT_DATE - INTERVAL '10 days')
    ORDER BY company_id, valuation_date DESC
)

SELECT
    -- Company
    lmc.company_id,
    e.name AS company_name,

    -- Valuation
    lmc.valuation_date,
    lmc.shares_outstanding,
    lmc.market_cap,

    -- Currency
    lmc.currency_id,
    ccy.code AS currency_code,

    -- USD conversion
    fx.mid_rate AS fx_rate_to_usd,
    lmc.market_cap * fx.mid_rate AS market_cap_usd,

    -- Metadata
    lmc.loaded_at
FROM latest_market_cap lmc
JOIN {{ source('master', 'entity') }} e
    ON e.entity_id = lmc.company_id
JOIN {{ source('master', 'currency') }} ccy
    ON ccy.currency_id = lmc.currency_id
JOIN {{ source('master', 'currency') }} usd
    ON usd.code = 'USD'
JOIN {{ source('master', 'fx_rate') }} fx
    ON fx.base_currency_id = lmc.currency_id
    AND fx.quote_currency_id = usd.currency_id
    AND fx.rate_date = lmc.valuation_date
    AND fx.rate_date > (CURRENT_DATE - INTERVAL '10 days')
