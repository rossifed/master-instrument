{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views","vw_equity_market_cap"],
            "group": "market"
        }
    }
) }}

-- Equity Market Cap: Forward-filled shares × close price (instrument level)
-- Uses range join for efficient forward-fill of shares outstanding

WITH share_ranges AS (
    -- Pre-compute validity ranges for shares outstanding
    SELECT 
        so.equity_id,
        so.date AS start_date,
        COALESCE(
            (LEAD(so.date) OVER (PARTITION BY so.equity_id ORDER BY so.date) - INTERVAL '1 day')::date,
            '3000-01-01'::date
        ) AS end_date,
        so.number_of_shares
    FROM {{ source('master', 'share_outstanding') }} so
)

SELECT 
    q.instrument_id AS equity_id,
    md.trade_date AS valuation_date,
    md.close AS close_price,
    sr.number_of_shares AS shares_outstanding,
    (md.close * sr.number_of_shares) AS market_cap,
    (sr.start_date = md.trade_date) AS is_shares_publication_date,
    md.currency_id
FROM {{ source('master', 'market_data') }} md
JOIN {{ source('master', 'quote') }} q 
    ON q.quote_id = md.quote_id
    AND q.is_primary = TRUE  -- Only primary quote for market cap
JOIN share_ranges sr 
    ON sr.equity_id = q.instrument_id
    AND md.trade_date BETWEEN sr.start_date AND sr.end_date
