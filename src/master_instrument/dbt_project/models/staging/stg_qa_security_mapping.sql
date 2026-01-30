{{ config(
    materialized = "table",
    indexes=[
        {'columns': ['ds_info_code'], 'unique': False},
        {'columns': ['rkd_code'], 'unique': False},
        {'columns': ['ds_sec_code'], 'unique': False}
    ],
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_security_mapping"],
            "group": "reference"
        }
    }
) }}

SELECT DISTINCT
    dssec."DsCmpyCode"       AS ds_compy_code,
    dssec."DsSecCode"        AS ds_sec_code,
    dssec."PrimQtInfoCode"   AS ds_sec_prim_info_code,
    (upper(trim(dssec."IsMajorSec")) = 'Y')::boolean AS ds_sec_is_major,
    dsctry."DelistDate"      AS ds_ctry_delist_date,
    dsctry."IsPrimQt"        AS ds_ctry_is_prim_qt,
    dsctry."InfoCode"        AS ds_info_code,
    rkdissue."IssueCode"     AS rkd_issue_code,
    rkdissue."IssueID"       AS rkd_issue_id,
    rkdissue."Code"          AS rkd_code,
    rkdcomp."RelToCode"      AS rkd_rel_to_code,
    'QA'                     AS source  
FROM {{ source('raw', 'qa_DS2Security') }} dssec
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} dsctry
  ON dsctry."DsSecCode" = dssec."DsSecCode"
JOIN {{ source('raw', 'qa_RKDFndCmpRefIssue') }} rkdissue
  ON rkdissue."seccode" = dsctry."seccode"
 AND rkdissue."typ"     = dsctry."typ"
JOIN {{ source('raw', 'qa_RKDFndCmpRef') }} rkdcomp
  ON rkdcomp."Code" = rkdissue."Code"
