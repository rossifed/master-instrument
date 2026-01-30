{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_equity"],
            "group": "reference"
        }
    }
) }}

SELECT DISTINCT
    sqsm.ds_info_code::TEXT            AS external_instrument_id,
    sqsm.rkd_code::TEXT                AS external_company_id,
    sqsm.ds_sec_code::TEXT             AS external_security_id,
    sqsm.ds_sec_code,
    sqsm.ds_compy_code,
    sqsm.rkd_issue_code,
    sqsm.rkd_issue_id,
    sqsm.rkd_code,
    sqsm.rkd_rel_to_code,
    dsc."DsCmpyName"                   AS company_name,
    ds."DsSctyCode"                    AS ds_scty_code,
    ds."DsSecName"                     AS ds_sec_name,
    (upper(trim(ds."IsMajorSec")) = 'Y')::boolean AS is_major_security,
    ds."ISOCurrCode"                   AS iso_curr_code,
    ds."DivUnit"                       AS div_unit,
    ds."PrimQtSedol"                   AS prim_qt_sedol,
    ds."PrimExchMnem"                  AS prim_exch_mnem,
    ds."PrimQtInfoCode"                AS prim_qt_info_code,
    ds."WSSctyPPI"                     AS ws_scty_ppi,
    ds."IBESTicker"                    AS ibes_ticker,
    ds."WSSctyPPI2"                    AS ws_scty_ppi2,
    ds."IBESTicker2"                   AS ibes_ticker2,
    ds."DelistDate"                    AS sec_delist_date,
    dsctry."Region"                    AS region,
    dsctry."DsQtName"                  AS ds_qt_name,
    (dsctry."IsPrimQt"  = 1)           AS is_primary_country,
    dsctry."StatusCode"                AS status_code,
    dsctry."RegCodeTypeId"             AS reg_code_type_id,
    dsctry."TypeCode"                  AS type_code,
    dsctry."DelistDate"                AS ctry_delist_date,
    split_part(rkdi."ric", '.', 1)     AS ric_root,
    split_part(rkdi."DisplayRIC", '.', 1)     AS display_ric_root,
    rkdi."IssueTypeCode"               AS issue_type_code,
    rkdi."IssueStatus"                 AS issue_status,
    rkdi."IssueOrder"                  AS issue_order,
    rkdi."IssueName"                   AS issue_description,
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
    rkdi."MostRecentSplit"             AS split_factor,
    rkdi."MostRecentSplitDt"           AS split_date,
    rkdi."ShPerListing"                AS sh_per_listing,
    -- Type IDs from seeds (scalar subqueries)
    (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
    (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ ref('stg_qa_security_mapping') }} AS sqsm
JOIN {{ source('raw', 'qa_DS2Security') }} AS ds
  ON ds."DsSecCode" = sqsm.ds_sec_code
JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} AS rkdi
  ON rkdi."IssueCode" = sqsm.rkd_issue_code
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} AS dsctry
  ON dsctry."InfoCode" = sqsm.ds_info_code
JOIN {{ source('raw', 'qa_DS2Company') }} AS dsc
  ON dsc."DsCmpyCode" = ds."DsCmpyCode"   