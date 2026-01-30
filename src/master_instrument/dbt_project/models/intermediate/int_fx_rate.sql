{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_fx_rate"],
            "group": "market"
        }
    }
) }}

-- Currency-specific quotation factors
-- FX rates in source are inverted, so we compute 1 / (rate * factor)
-- GBP is quoted in pence (1/100 of a pound), so factor is 100.0
WITH quotation_factors AS (
    SELECT * FROM (VALUES
        ('GBP', 100.0)   -- Pence to pounds
        -- Add other special cases here as needed
    ) AS t(currency_code, factor)
),

-- Calculate raw_mid_rate once (single source of truth)
fx_with_raw_mid AS (
    SELECT
        qdfr.ex_rate_date,
        qdfr.mid_rate,
        qdfr.bid_rate,
        qdfr.offer_rate,
        bc.currency_id AS base_currency_id,
        qc.currency_id AS quote_currency_id,
        COALESCE(qf.factor, 1.0) AS quotation_factor,
        -- Raw mid rate calculation - single definition
        COALESCE(qdfr.mid_rate, (qdfr.bid_rate + qdfr.offer_rate) / 2, qdfr.bid_rate, qdfr.offer_rate) AS raw_mid_rate
    FROM {{ ref('stg_qa_fx_rate') }} qdfr
    JOIN master.currency bc ON bc.code = qdfr.from_curr_code
    JOIN master.currency qc ON qc.code = qdfr.to_curr_code AND bc.currency_id != qc.currency_id
    LEFT JOIN quotation_factors qf ON qf.currency_code = qdfr.from_curr_code
),

-- Real FX rates (cross-currency only)
-- Filter on raw_mid_rate > 0 to exclude invalid rates
cross_currency_rates AS (
    SELECT
        ex_rate_date AS rate_date,
        base_currency_id,
        quote_currency_id,
        -- Apply conversion: 1 / (rate * factor) - inverts + applies quotation factor
        1.0 / (raw_mid_rate * quotation_factor) AS mid_rate,
        1.0 / NULLIF(bid_rate * quotation_factor, 0) AS bid_rate,
        1.0 / NULLIF(offer_rate * quotation_factor, 0) AS ask_rate,
        -- Data lineage columns
        quotation_factor,
        mid_rate AS mid_rate_raw,
        (mid_rate IS NULL) AS is_mid_rate_derived
    FROM fx_with_raw_mid
    WHERE raw_mid_rate > 0  -- Exclude invalid rates (NULL or 0)
)

-- Combine cross-currency rates with identity rates (same currency = 1.0)
-- Identity rates ensure joins always return a rate for same-currency conversions
SELECT * FROM cross_currency_rates
UNION ALL
SELECT DISTINCT
    rate_date,
    base_currency_id,
    base_currency_id AS quote_currency_id,
    1.0 AS mid_rate,
    1.0 AS bid_rate,
    1.0 AS ask_rate,
    1.0 AS quotation_factor,
    1.0 AS mid_rate_raw,
    FALSE AS is_mid_rate_derived
FROM cross_currency_rates
