-- Test: Quote currency must match the most recent market data currency in staging
-- The currency in stg_qa_quote should reflect the current trading currency
-- Returns quotes where currency doesn't match latest market data

WITH latest_market_data_currency AS (
    SELECT DISTINCT ON (md.external_quote_id)
        md.external_quote_id,
        md.iso_curr_code AS latest_md_currency,
        md.date AS latest_trade_date
    FROM {{ ref('stg_qa_market_data') }} md
    WHERE md.iso_curr_code IS NOT NULL
    ORDER BY md.external_quote_id, md.date DESC
)

SELECT
    q.external_quote_id,
    q.external_instrument_id,
    q.iso_curr_code AS quote_currency,
    lmc.latest_md_currency,
    lmc.latest_trade_date,
    'currency_mismatch' AS violation
FROM {{ ref('stg_qa_quote') }} q
JOIN latest_market_data_currency lmc
    ON lmc.external_quote_id = q.external_quote_id
WHERE q.iso_curr_code != lmc.latest_md_currency
