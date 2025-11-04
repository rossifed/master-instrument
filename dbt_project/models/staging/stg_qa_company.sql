{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_company"]
        }
    }
) }}

SELECT DISTINCT
    sqsm.rkd_code::TEXT          AS external_id,
    sqsm.rkd_code,
    sqsm.rkd_rel_to_code,

    qrcr."PrimaryName"           AS primary_name,
    qrcf."TxtInfo"               AS description,
    qrcr."LatestFinAnnDt"        AS latest_fin_ann_dt,
    qrcr."LastModFinDt"          AS last_mod_fin_dt,
    qrcr."LastModOtherDt"        AS last_mod_other_dt,
    qrcr."IssuerTypeCode"        AS issuer_type_code,
    qrcr."COATypeCode"           AS coa_type_code,
    qrcr."FinStmtCurrCode"       AS fin_stmt_curr_code,
    qrcr."EstCurrCode"           AS est_curr_code, 
  
    qrcd."Employees"             AS employees,
    qrcd."EmpLastUpdDt"          AS emp_last_upd_dt,
    qrcd."StAdd1"                AS st_add_1,
    qrcd."StAdd2"                AS st_add_2,
    qrcd."Post"                  AS post,
    qrcd."StateOrReg"            AS state,
    qrcd."City"                  AS city,
    qrcd."ISOCntryCode"          AS iso_cntry_code,
    qrcd."PublicSince"           AS public_since,
    qrcd."ComShHldr"             AS com_sh_hldr,
    qrcd."ComShHldrDt"           AS com_sh_hldr_dt,
    qrcd."TotShOut"              AS tot_sh_out,
    qrcd."TotShOutDt"            AS tot_sh_out_dt,
    qrcd."TotFloat"              AS tot_float,

    'QA'                         AS source
FROM {{ ref('stg_qa_security_mapping') }} AS sqsm
JOIN {{ source('raw', 'qa_RKDFndCmpRef') }} AS qrcr
  ON qrcr."Code" = sqsm.rkd_code
LEFT JOIN {{ source('raw', 'qa_RKDFndCmpDet') }} AS qrcd
  ON qrcd."Code" = sqsm.rkd_code
LEFT JOIN {{ source('raw', 'qa_RKDFndCmpFiling') }} AS qrcf
  ON qrcf."Code" = qrcd."Code"
 AND qrcf."TxtInfoTypeCode" = 2
