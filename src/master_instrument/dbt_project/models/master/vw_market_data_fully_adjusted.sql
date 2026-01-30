{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_market_data_fully_adjusted"],
            "group": "market"
        }
    }
) }}

-- Applies both corporate action AND dividend adjustments to market data
-- Combined adjustment factor = cum_adj_factor * cum_div_factor

WITH corp_ranges AS (
    -- Corporate actions with validity ranges (adj_type = 2 for full adjustments)
    SELECT 
        ca.equity_id,
        ca.adj_date AS start_date,
        COALESCE(ca.end_adj_date, '3000-01-01'::date) AS end_date,
        ca.cum_adj_factor 
    FROM {{ source('master', 'corpact_adjustment') }} ca 
    WHERE ca.adj_type = 2
),

div_ranges AS (
    -- Dividend adjustments with validity ranges
    SELECT 
        da.equity_id,
        COALESCE(
            LAG(da.ex_div_date) OVER (PARTITION BY da.equity_id ORDER BY da.ex_div_date),
            '1900-01-01'::date
        ) AS start_date,
        (da.ex_div_date - INTERVAL '1 day')::date AS end_date,
        da.cum_div_factor
    FROM {{ source('master', 'dividend_adjustment') }} da
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
    -- Individual factors
    COALESCE(cr.cum_adj_factor, 1.0) AS cum_adj_factor,
    COALESCE(dr.cum_div_factor, 1.0) AS cum_div_factor,
    -- Combined factor
    COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0) AS cum_full_factor,
    -- Raw prices
    md.open,
    md.high,
    md.low,
    md.close,
    md.bid,
    md.ask,
    md.vwap,
    md.volume,
    -- Fully adjusted prices (both factors applied, normalized by price_unit)
    (md.open * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_open,
    (md.high * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_high,
    (md.low * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_low,
    (md.close * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_close,
    md.volume / COALESCE(cr.cum_adj_factor, 1.0) AS adjusted_volume,  -- Only corp actions affect volume
    (md.bid * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_bid,
    (md.ask * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_ask,
    (md.vwap * COALESCE(cr.cum_adj_factor, 1.0) * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_vwap
FROM {{ ref('vw_market_data_base') }} md
LEFT JOIN corp_ranges cr
    ON cr.equity_id = md.instrument_id  
    AND md.trade_date BETWEEN cr.start_date AND cr.end_date
LEFT JOIN div_ranges dr
    ON dr.equity_id = md.instrument_id
    AND md.trade_date BETWEEN dr.start_date AND dr.end_date
