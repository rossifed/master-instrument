{{ config(
    materialized="view",
    meta={
        "dagster": {
            "asset_key": ["staging", "stg_qa_instrument"]
        }
    }
) }}

SELECT
    s."DsSecCode" AS security_id,
    s."IsMajorSec" AS is_major_security,
    q."InfoCode" AS external_identifier,
    q."TypeCode" AS type_code,
    q."DsMnem" AS refinitiv_mnemonic,
    q."Region" AS iso_country,
    q."PrimISOCurrCode" AS iso_currency,
    CAST(q."DelistDate" AS DATE) AS delist_date,
    c."DsCmpyName" AS name,
    i."Ticker" AS ticker,
    i."ric" AS ric,
    i."isin" AS isin,
    i."Sedol" AS sedol,
    i."Cusip" AS cusip,
    i."Code" AS rkd_code,
    c."DsCmpyName" AS company_name,
    c."DsCmpyCode" AS company_code,
    c."CmpyCtryCode" AS company_country,
    c."CmpyCtryType" AS company_country_type,
    f."TxtInfo" AS description,
    e."ExchIntCode" AS exchange_id,
    e."ExchName" AS exchange_name,
    eq."ISOCurrCode" AS exchange_currency,
    eq."InfoCode" AS quote_code,
    eq."IsPrimExchQt" AS is_primary_exchange,
    t."Code" AS instrument_type_code,
    t."Desc_" AS instrument_type_name,
    r."COATypeCode" AS coa_type_code,
    d."City" AS company_city,
    d."Employees" AS company_employees,
    d."StAdd1" AS company_address_1,
    d."StAdd2" AS company_address_2,
    d."Post" AS company_postal_code,
    d."ISOCntryCode" AS company_iso_country,
    'QA' AS source
FROM {{ source('raw', 'qa_DS2CtryQtInfo') }} q

JOIN {{ source('raw', 'qa_DS2ExchQtInfo') }} eq
    ON q."InfoCode" = eq."InfoCode"
    AND eq."IsPrimExchQt" = 'Y'

JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} i
    ON q."seccode" = i."seccode"
    AND q."typ" = i."typ"

JOIN {{ source('raw', 'qa_DS2Security') }} s
    ON q."DsSecCode" = s."DsSecCode"

JOIN {{ source('raw', 'qa_DS2Company') }} c
    ON s."DsCmpyCode" = c."DsCmpyCode"

JOIN {{ source('raw', 'qa_RKDFndCmpFiling') }} f
    ON f."TxtInfoTypeCode" = 2
    AND f."Code" = i."Code"

JOIN {{ source('raw', 'qa_DS2Exchange') }} e
    ON e."ExchIntCode" = eq."ExchIntCode"

LEFT JOIN {{ source('raw', 'qa_RKDFndCmpDet') }} d
    ON d."Code" = i."Code"

LEFT JOIN {{ source('raw', 'qa_RKDFndCmpRef') }} r
    ON r."Code" = d."Code"


JOIN {{ source('raw', 'qa_DS2XRef') }} t
    ON t."Code" = q."TypeCode"
    AND t."Type_" = 2
