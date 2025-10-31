{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_security_mapping"]
        }
    }
) }}

SELECT DISTINCT
    dssec."DsCmpyCode"       AS ds_compy_code,
    dssec."DsSecCode"        AS ds_sec_code,
    dssec."PrimQtInfoCode"   AS ds_sec_prim_info_code,
    dsctry."DelistDate"      AS ds_ctry_delist_date,
    dsctry."IsPrimQt"        AS ds_ctry_is_prim_qt,
    dsctry."InfoCode"        AS ds_info_code,
    rkdissue."IssueCode"     AS rkd_issue_code,
    rkdissue."IssueID"       AS rkd_issue_id,
    rkdissue."Code"          AS rkd_code,
    rkdcomp."RelToCode"      AS rkd_rel_to_code
FROM {{ source('raw', 'qa_DS2Security') }} dssec
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} dsctry
  ON dsctry."DsSecCode" = dssec."DsSecCode"
JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} rkdissue
  ON dsctry."seccode" = rkdissue."seccode"
 AND dsctry."typ"     = rkdissue."typ"
JOIN {{ source('raw', 'qa_RKDFndCmpRef') }} rkdcomp
  ON rkdcomp."Code" = rkdissue."Code"
