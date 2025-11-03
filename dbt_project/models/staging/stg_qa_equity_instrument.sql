{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_equity_instrument"]
        }
    }
) }}

SELECT DISTINCT
    sqsm.ds_info_code::TEXT as external_instrument_id,
    sqsm.rkd_code::TEXT as external_company_id,
    sqsm.ds_sec_code,
    sqsm.ds_compy_code,
    sqsm.rkd_issue_code,
    sqsm.rkd_issue_id,
    sqsm.rkd_code,
    sqsm.rkd_rel_to_code,
    ds."DsSctyCode",
    ds."DsSecName",
    ds."IsMajorSec",
    ds."ISOCurrCode",
    ds."DivUnit",
    ds."PrimQtSedol",
    ds."PrimExchMnem",
    ds."PrimQtInfoCode",
    ds."WSSctyPPI",
    ds."IBESTicker",
    ds."WSSctyPPI2",
    ds."IBESTicker2",
    ds."DelistDate" AS ds_delist_date,
    dsctry."DsQtName",
    rkdi."IssueTypeCode",
    rkdi."IssueStatus",
    rkdi."IssueOrder",
    rkdi."IssueName",
    rkdi."ListingTypeCode",
    rkdi."Ticker",
    rkdi."Cusip",
    rkdi."isin",
    rkdi."ric",
    rkdi."DisplayRIC",
    rkdi."Sedol",
    rkdi."ExchCode",
    rkdi."ExchCtryCode",
    rkdi."RegionCode",
    rkdi."DFlag",
    rkdi."InstrPI",
    rkdi."QuotePI",
    rkdi."MostRecentSplit",
    rkdi."MostRecentSplitDt",
    rkdi."ShPerListing"
FROM {{ ref('stg_qa_security_mapping') }} sqsm
JOIN {{ source('raw', 'qa_DS2Security') }} ds
  ON ds."DsSecCode" = sqsm.ds_sec_code
JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} rkdi
  ON rkdi."IssueCode" = sqsm.rkd_issue_code
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} dsctry
  ON dsctry."InfoCode" = sqsm."ds_info_code"

