{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_market_data_corp_adjusted"],
            "group": "market"
        }
    }
) }}

-- Corporate action adjusted market data
-- Uses LEFT JOIN with COALESCE for default factor (1.0) when no adjustment exists

WITH adj_ranges AS (
    -- Corporate actions with adj_type = 2 (full adjustments) with date ranges
    SELECT 
        ca.equity_id,
        ca.adj_date AS start_date,
        COALESCE(ca.end_adj_date, '3000-01-01'::date) AS end_date,
        ca.cum_adj_factor 
    FROM {{ source('master', 'corpact_adjustment') }} ca 
    WHERE ca.adj_type = 2
)

SELECT 
    md.quote_id,
    md.trade_date,
    md.instrument_id,
    md.instrument_name,
    md.entity_id,
    md.entity_name,
    md.currency_id,
    md.currency_code,
    md.is_primary,
    md.price_unit,
    COALESCE(ar.cum_adj_factor, 1.0) AS cum_adj_factor,
    -- Raw prices
    md.open,
    md.high,
    md.low,
    md.close,
    md.bid,
    md.ask,
    md.vwap,
    md.volume,
    -- Adjusted prices (factor applied, normalized by price_unit)
    (md.open * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_open,
    (md.high * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_high,
    (md.low * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_low,
    (md.close * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_close,
    md.volume / COALESCE(ar.cum_adj_factor, 1.0) AS adjusted_volume,
    (md.bid * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_bid,
    (md.ask * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_ask,
    (md.vwap * COALESCE(ar.cum_adj_factor, 1.0)) / md.price_unit AS adjusted_vwap
FROM {{ ref('vw_market_data_base') }} md
LEFT JOIN adj_ranges ar
    ON ar.equity_id = md.instrument_id  
    AND md.trade_date BETWEEN ar.start_date AND ar.end_date
