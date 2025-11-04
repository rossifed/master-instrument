{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_equity"]
        }
    }
) }}

SELECT DISTINCT
    sqsm.ds_info_code::TEXT            AS external_instrument_id,
    sqsm.rkd_code::TEXT                AS external_company_id,
    sqsm.ds_sec_code,
    sqsm.ds_compy_code,
    sqsm.rkd_issue_code,
    sqsm.rkd_issue_id,
    sqsm.rkd_code,
    sqsm.rkd_rel_to_code,

    ds."DsSctyCode"                    AS ds_scty_code,
    ds."DsSecName"                     AS ds_sec_name,
    ds."IsMajorSec"                    AS is_major_sec,
    ds."ISOCurrCode"                   AS iso_curr_code,
    ds."DivUnit"                       AS div_unit,
    ds."PrimQtSedol"                   AS prim_qt_sedol,
    ds."PrimExchMnem"                  AS prim_exch_mnem,
    ds."PrimQtInfoCode"                AS prim_qt_info_code,
    ds."WSSctyPPI"                     AS ws_scty_ppi,
    ds."IBESTicker"                    AS ibes_ticker,
    ds."WSSctyPPI2"                    AS ws_scty_ppi2,
    ds."IBESTicker2"                   AS ibes_ticker2,
    ds."DelistDate"                    AS ds_delist_date,

    dsctry."DsQtName"                  AS ds_qt_name,
    dsctry."Region"                    AS region,

    rkdi."IssueTypeCode"               AS issue_type_code,
    rkdi."IssueStatus"                 AS issue_status,
    rkdi."IssueOrder"                  AS issue_order,
    rkdi."IssueName"                   AS issue_name,
    rkdi."ListingTypeCode"             AS listing_type_code,
    rkdi."Ticker"                      AS ticker,
    rkdi."Cusip"                       AS cusip,
    rkdi."isin",
    rkdi."ric",
    rkdi."DisplayRIC"                  AS display_ric,
    rkdi."Sedol"                       AS sedol,
    rkdi."ExchCode"                    AS exch_code,
    rkdi."ExchCtryCode"                AS exch_try_code,
    rkdi."RegionCode"                  AS region_code,
    rkdi."DFlag"                       AS d_flag,
    rkdi."InstrPI"                     AS instr_pi,
    rkdi."QuotePI"                     AS quote_pi,
    rkdi."MostRecentSplit"             AS most_recent_split,
    rkdi."MostRecentSplitDt"           AS most_recent_split_dt,
    rkdi."ShPerListing"                AS sh_per_listing,
    'QA'                               AS source
FROM {{ ref('stg_qa_security_mapping') }} AS sqsm
JOIN {{ source('raw', 'qa_DS2Security') }} AS ds
  ON ds."DsSecCode" = sqsm.ds_sec_code
JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} AS rkdi
  ON rkdi."IssueCode" = sqsm.rkd_issue_code
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} AS dsctry
  ON dsctry."InfoCode" = sqsm.ds_info_code
