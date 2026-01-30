{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_dividend_adjustment"],
            "group": "reference"
        }
    }
) }}

WITH fx_rates AS (
    SELECT * 
    FROM master.fx_rate
    WHERE mid_rate > 0
),
div_raw AS (
    SELECT
        d.equity_id,
        d.effective_date AS ex_div_date,
        p.trade_date AS price_date,
        p.close AS close,
        p.currency_id AS price_currency_id,
        d.dividend_rate,
        d.currency_id AS div_currency_id,
        CASE
            WHEN d.currency_id = p.currency_id THEN 1.0
            ELSE fx.mid_rate 
        END AS fx_rate,
        CASE
            WHEN d.currency_id = p.currency_id THEN d.dividend_rate 
            WHEN fx.mid_rate IS NOT NULL THEN d.dividend_rate * fx.mid_rate
            ELSE NULL
        END AS div_converted
    FROM master.dividend d
    JOIN master.quote q ON q.instrument_id = d.equity_id AND q.is_primary = true
    LEFT JOIN LATERAL (
        SELECT
            cc.trade_date,
            cc."close",
            cc.currency_id 
        FROM master.market_data cc
        WHERE cc.quote_id = q.quote_id 
          AND cc.trade_date < d.effective_date 
          AND cc."close" IS NOT NULL
        ORDER BY cc.trade_date DESC
        LIMIT 1
    ) p ON TRUE
    LEFT JOIN LATERAL (
        SELECT 
            fr.mid_rate 
        FROM fx_rates fr
        WHERE fr.base_currency_id = d.currency_id 
          AND fr.quote_currency_id = p.currency_id 
          AND fr.rate_date <= p.trade_date 
        ORDER BY fr.rate_date DESC
        LIMIT 1
    ) fx ON TRUE
    WHERE p.close IS NOT NULL                           
      AND (d.currency_id = p.currency_id OR fx.mid_rate IS NOT NULL)
),
div_factors AS (
    SELECT
        equity_id,
        ex_div_date,
        price_date,
        close,
        price_currency_id,
        SUM(div_converted) AS total_div_converted,
        (close - SUM(div_converted)) / close AS div_adj_factor
    FROM div_raw
    GROUP BY equity_id, ex_div_date, price_date, close, price_currency_id
    -- Filter out invalid cases where dividend >= price (data quality issue)
    HAVING SUM(div_converted) < close
)
SELECT
    equity_id,
    ex_div_date,
    price_date,
    close,
    price_currency_id,
    total_div_converted,
    div_adj_factor,
    EXP(
        SUM(LN(div_adj_factor)) OVER (
            PARTITION BY equity_id
            ORDER BY ex_div_date DESC
        )
    ) AS cum_div_factor
FROM div_factors
