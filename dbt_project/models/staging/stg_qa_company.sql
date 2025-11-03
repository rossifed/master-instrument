{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_company"]
        }
    }
) }}

    SELECT DISTINCT 
        sqsm.rkd_code::TEXT as external_id,
        sqsm.rkd_code,
        sqsm.rkd_rel_to_code,
        qrcr."PrimaryName",
        qrcf."TxtInfo" AS description,
        qrcr."LatestFinAnnDt",
        qrcr."LastModFinDt",
        qrcr."IssuerTypeCode",
        qrcr."COATypeCode",
        qrcd."Employees",
        qrcd."EmpLastUpdDt",
        qrcd."StAdd1",
        qrcd."StAdd2",
        qrcd."Post",
        qrcd."City",
        qrcd."PublicSince",
        qrcd."ISOCntryCode"
    FROM {{ ref('stg_qa_security_mapping') }} sqsm
    JOIN {{ source('raw', 'qa_RKDFndCmpRef') }} AS qrcr
      ON qrcr."Code" = sqsm.rkd_code
    LEFT JOIN {{ source('raw', 'qa_RKDFndCmpDet') }} AS qrcd
      ON qrcd."Code" = sqsm.rkd_code
    LEFT JOIN {{ source('raw', 'qa_RKDFndCmpFiling') }} AS qrcf
      ON qrcf."Code" = qrcd."Code"
     AND qrcf."TxtInfoTypeCode" = 2
