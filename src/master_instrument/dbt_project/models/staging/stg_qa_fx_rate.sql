{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","market","stg_qa_fx_rate"],
            "group": "market"
        }
    }
) }}

SELECT 
    qdfr."ExRateDate" AS ex_rate_date,
    qdfr."FromCurrCode" AS from_curr_code,
    qdfr."ToCurrCode" AS to_curr_code,
    qdfr."MidRate" AS mid_rate,
    qdfr."BidRate" AS bid_rate,
    qdfr."OfferRate" AS offer_rate,
    'QA' AS source
FROM {{ source('raw', 'qa_DS2FxRate') }} qdfr 
WHERE qdfr."ExRateDate" > '{{ var('min_data_date') }}'::date
  -- Filter out rows with no usable rate data
  AND NOT (qdfr."MidRate" IS NULL 
           AND qdfr."BidRate" IS NULL 
           AND qdfr."OfferRate" IS NULL)
