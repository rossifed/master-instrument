{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_corpact_event"],
            "group": "reference"
        }
    }
) }}



SELECT 
    evt."InfoCode"::TEXT          AS external_instrument_id,
    evt."EventNum"                AS event_sequence,
    evt."ActionTypeCode"          AS corpact_type_code,
    evt."ResInfoCode"::TEXT       AS res_external_instrument_id,
    evt."NumNewShares"            AS new_shares_count,
    evt."NumOldShares"            AS old_shares_count,
    evt."ISOCurrCode"             AS currency_code,
    evt."CashAmt"                 AS cash_amount,
    evt."OfferCmpyName"           AS offer_company_name,
    evt."AnnouncedDate"::DATE     AS announced_date,
    evt."RecordDate"::DATE        AS record_date,
    evt."EffectiveDate"::DATE     AS effective_date,
    evt."ExpiryDate"::DATE        AS expiry_date,
    -- Type IDs from seeds (scalar subqueries for optimizer)
    (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2CapEvent') }} evt
JOIN {{ ref('stg_qa_security_mapping') }} sqsm 
    ON sqsm.ds_info_code = evt."InfoCode"
WHERE evt."EffectiveDate" >= '{{ var('min_data_date') }}'::date
