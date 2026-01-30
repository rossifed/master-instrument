{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views","vw_company_market_cap"],
            "group": "market"
        }
    }
) }}

-- Company Market Cap: Simple view on company_market_cap table
-- Consolidated market cap across all share classes
-- Includes USD conversion using fx_rate (identity rates ensure join always works)

SELECT 
    cmc.valuation_date,
    cmc.company_id,
    e.name AS company_name,
    cmc.shares_outstanding,
    cmc.market_cap,
    cmc.currency_id,
    c.code AS currency_code,
    -- USD conversion (identity rates handle same-currency case)
    fx.mid_rate AS fx_rate_to_usd,
    cmc.market_cap * fx.mid_rate AS market_cap_usd
FROM {{ source('master', 'company_market_cap') }} cmc
JOIN {{ source('master', 'entity') }} e 
    ON e.entity_id = cmc.company_id
JOIN {{ source('master', 'currency') }} c 
    ON c.currency_id = cmc.currency_id
JOIN {{ source('master', 'currency') }} usd 
    ON usd.code = 'USD'
JOIN {{ source('master', 'fx_rate') }} fx 
    ON fx.base_currency_id = cmc.currency_id
    AND fx.quote_currency_id = usd.currency_id
    AND fx.rate_date = cmc.valuation_date
