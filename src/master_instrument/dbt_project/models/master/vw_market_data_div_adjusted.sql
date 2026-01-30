{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_market_data_div_adjusted"],
            "group": "market"
        }
    }
) }}

-- Applies dividend adjustments to market data
-- Same pattern as vw_market_data_corp_adjusted: LEFT JOIN + COALESCE

WITH div_ranges AS (
    -- Compute validity ranges: from previous ex_div_date to current ex_div_date - 1 day
    -- The cum_div_factor applies to all dates BEFORE the ex_div_date
    SELECT 
        da.equity_id,
        COALESCE(
            LAG(da.ex_div_date) OVER (PARTITION BY da.equity_id ORDER BY da.ex_div_date),
            '1900-01-01'::date
        ) AS start_date,
        (da.ex_div_date - INTERVAL '1 day')::date AS end_date,
        da.cum_div_factor,
        da.div_adj_factor
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
    COALESCE(dr.cum_div_factor, 1.0) AS cum_div_factor,
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
    (md.open * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_open,
    (md.high * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_high,
    (md.low * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_low,
    (md.close * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_close,
    md.volume AS adjusted_volume,  -- Volume not affected by dividends
    (md.bid * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_bid,
    (md.ask * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_ask,
    (md.vwap * COALESCE(dr.cum_div_factor, 1.0)) / md.price_unit AS adjusted_vwap
FROM {{ ref('vw_market_data_base') }} md
LEFT JOIN div_ranges dr
    ON dr.equity_id = md.instrument_id
    AND md.trade_date BETWEEN dr.start_date AND dr.end_date
