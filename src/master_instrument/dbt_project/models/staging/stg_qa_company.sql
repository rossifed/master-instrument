{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_company"],
            "group": "reference"
        }
    }
) }}
WITH parent_orga AS (
    SELECT
        por."OrgPermID"          AS org_perm_id,
        por."ComName"            AS company_name,
        por."PriRptEntityCode"   AS primary_rpt_entity_code,
        rfi."Code"               AS rkd_code,
        upopi."OrgPermID"        AS ultimate_org_perm_id,
        upopi."ComName"          AS ultimate_company_name,
        upopi."PriRptEntityCode" AS ultimate_rpt_entity_code,
        urfi."Code"              AS ultimate_rkd_code
    FROM {{ source('raw', 'qa_PermOrgRef') }} AS por
    JOIN {{ source('raw', 'qa_RKDFndInfo') }}  AS rfi
      ON rfi."RepNo" = por."PriRptEntityCode"
    JOIN {{ source('raw', 'qa_PermOrgRef') }}  AS upopi
      ON upopi."OrgPermID" = por."UltimateParentOrgPermID"
    JOIN {{ source('raw', 'qa_RKDFndInfo') }}  AS urfi
      ON urfi."RepNo" = upopi."PriRptEntityCode"
    WHERE rfi."Code" <> urfi."Code"
),

primary_phone AS (
    SELECT DISTINCT ON (qrcp."Code")
        qrcp."Code" AS rkd_code,
        CONCAT('00', qrcp."CtryPh", qrcp."City", qrcp."PhoneNo") AS phone_number
    FROM {{ source('raw', 'qa_RKDFndCmpPhone') }} AS qrcp
    WHERE qrcp."PhTypeCode" = 1
        AND qrcp."PhoneNo" IS NOT NULL
    ORDER BY qrcp."Code"
)
SELECT
        qrcr."Code"::TEXT                                           AS external_company_id,
        qrcr."Code"                                                 AS rkd_code,
        qrcr."RelToCode"::TEXT                                      AS rkd_rel_to_code,
        trim(qrcr."PrimaryName")                                    AS primary_name,
        qrcf."TxtInfo"                                              AS description,
        qrcr."LatestFinAnnDt"                                       AS latest_fin_ann_dt,
        qrcr."LastModFinDt"                                         AS last_mod_fin_dt,
        qrcr."LastModOtherDt"                                       AS last_mod_other_dt,
        qrcr."IssuerTypeCode"                                       AS issuer_type_code,
        qrcr."COATypeCode"                                          AS coa_type_code,
        qrcr."OrgId"                                                AS organization_id,
        stat_ccy."Desc_"                                            AS fin_stmt_curr_code,
        est_ccy."Desc_"                                             AS est_curr_code,
        qrcd."Employees"                                            AS employees,
        qrcd."EmpLastUpdDt"                                         AS emp_last_upd_dt,
        NULLIF(initcap(lower(trim(qrcd."StAdd1"))), '')             AS st_add_1,
        NULLIF(initcap(lower(trim(qrcd."StAdd2"))), '')             AS st_add_2,
        upper(trim(qrcd."Post"))                                    AS post,
        upper(trim(qrcd."StateOrReg"))                              AS state,
        NULLIF(
            initcap(lower(trim(regexp_replace(qrcd."City", '\d', '', 'g')))),
            ''
        )                                                           AS city,
        qrcd."ISOCntryCode"                                         AS iso_cntry_code,
        qrcd."PublicSince"                                          AS public_since,
        qrcd."ComShHldr"                                            AS com_sh_hldr,
        qrcd."ComShHldrDt"                                          AS com_sh_hldr_dt,
        qrcd."TotShOut"                                             AS tot_sh_out,
        qrcd."TotShOutDt"                                           AS tot_sh_out_dt,
        qrcd."TotFloat"                                             AS tot_float,
        po.ultimate_rkd_code::TEXT                                  AS ultimate_organization_code,
        pp.phone_number                                             AS phone,
        -- Type IDs from seeds (scalar subqueries)
        (SELECT entity_type_id FROM {{ ref('entity_type') }} WHERE mnemonic = 'CMPY') AS entity_type_id,
        (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
    FROM {{ source('raw', 'qa_RKDFndCmpRef') }} AS qrcr
    LEFT JOIN parent_orga                          AS po
      ON po.rkd_code = qrcr."Code"
    LEFT JOIN {{ source('raw', 'qa_RKDFndCmpDet') }}   AS qrcd
      ON qrcd."Code" = qrcr."Code"
    LEFT JOIN {{ source('raw', 'qa_RKDFndCmpFiling') }} AS qrcf
      ON qrcf."Code" = qrcd."Code"
     AND qrcf."TxtInfoTypeCode" = 2
    LEFT JOIN {{ source('raw', 'qa_RKDFndCode') }}      AS est_ccy
      ON est_ccy."Code" = qrcr."EstCurrCode"
     AND est_ccy."Type_" = 58
    LEFT JOIN {{ source('raw', 'qa_RKDFndCode') }}      AS stat_ccy
      ON stat_ccy."Code" = qrcr."FinStmtCurrCode"
     AND stat_ccy."Type_" = 58
    LEFT JOIN primary_phone AS pp
      ON pp.rkd_code = qrcr."Code"

