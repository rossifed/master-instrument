{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_quote"]
        }
    }
) }}

SELECT DISTINCT
    qdeqi."InfoCode"::TEXT || '-' || qdeqi."ExchIntCode"::TEXT  AS external_quote_id,
    qdeqi."InfoCode"::TEXT                                     AS external_instrument_id,
    qdeqi."ExchIntCode"::TEXT                                  AS external_venue_id,

    qdcqi."DsSecCode"                                          AS ds_sec_code,
    qdcqi."Region"                                             AS region,
    qdcqi."RegCodeTypeId"                                      AS reg_code_type_id,
    qdcqi."IsPrimQt"                                           AS is_prim_qt,
    qdcqi."DsQtName"                                           AS ds_qt_name,
    qdcqi."DsLocalCode"                                        AS ds_local_code,
    qdcqi."DsMnem"                                             AS ds_mnem,
    qdcqi."CovergCode"                                         AS coverg_code,
    qdcqi."StatusCode"                                         AS status_code,
    qdcqi."PermID"                                             AS perm_id,
    qdcqi."TypeCode"                                           AS type_code,
    qdcqi."PrimISOCurrCode"                                    AS prim_iso_curr_code,
    qdcqi."DelistDate"                                         AS delist_date,
    qdcqi."startdate"                                          AS start_date,
    qdcqi."enddate"                                            AS end_date,

    qdeqi."InfoCode"                                           AS info_code,
    qdeqi."ISOCurrCode"                                        AS iso_curr_code,
    (upper(trim(qdeqi."IsPrimExchQt")) = 'Y')::boolean         AS is_primary,
    CASE
        WHEN qdeqi."PriceUnit" LIKE 'E%' THEN
            POWER(10, -CAST(SUBSTRING(qdeqi."PriceUnit" FROM 3 FOR 3) AS INT))
        ELSE 1
    END                                                        AS price_unit,
    qdeqi."StartDate"                                          AS start_date_quote,
    qdeqi."AltDsCode"                                          AS alt_ds_code,
    qdeqi."QtPermID"                                           AS qt_perm_id,
    qdeqi."mic"                                                AS mic,
    qdeqi."MICDesc"                                            AS mic_desc,

    qde."ExchIntCode"                                          AS exch_int_code,
    qde."DsExchCode"                                           AS ds_exch_code,
    qde."ExchType"                                             AS exch_type,
    qde."ExchName"                                             AS exch_name,
    qde."ExchMnem"                                             AS exch_mnem,
    qde."ExchCtryCode"                                         AS exch_ctry_code,
    qde."CtryCodeType"                                         AS ctry_code_type,
    'QA'                                                       AS source
FROM {{ ref('stg_qa_security_mapping') }} AS sqsm
JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} AS qdcqi
  ON qdcqi."InfoCode" = sqsm.ds_info_code
JOIN {{ source('raw', 'qa_DS2ExchQtInfo') }} AS qdeqi
  ON qdeqi."InfoCode" = qdcqi."InfoCode"
JOIN {{ source('raw', 'qa_DS2Exchange') }} AS qde
  ON qde."ExchIntCode" = qdeqi."ExchIntCode"
