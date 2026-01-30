{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_quote"],
            "group": "reference"
        }
    }
) }}

WITH unique_perm_match AS MATERIALIZED (
    SELECT
        "DsQuoteNumber",
        "ExchCode",
        MAX("LotSize")      AS perm_lot_size,
        MAX("IsPrimary")    AS perm_is_primary,
        MAX("InstrPermID")  AS perm_instr_id,
        MAX("ComName")      AS perm_name,
        MAX("Ticker")       AS perm_ticker,
        MAX("Mic")          AS perm_mic,
        MAX("QuotePermID")  AS perm_quote_id,
        MAX("ric")          AS perm_ric
    FROM {{ source('raw', 'qa_PermQuoteRef') }}
    GROUP BY "DsQuoteNumber", "ExchCode"
    HAVING COUNT(*) = 1
)
SELECT DISTINCT
        qdeqi."InfoCode"::text || '-' || qdeqi."ExchIntCode"::text AS external_quote_id,
        qdeqi."InfoCode"::text                                    AS external_instrument_id,
        qdeqi."ExchIntCode"::text                                 AS external_venue_id,
        qdcqi."DsSecCode"                                         AS ds_sec_code,
        qdcqi."DsCode"                                            AS ds_code,
        qdcqi."Region"                                            AS region,
        qdcqi."RegCodeTypeId"                                     AS reg_code_type_id,
        qdcqi."IsPrimQt"                                          AS is_primary_country,
        qdcqi."DsQtName"                                          AS ds_qt_name,
        qdcqi."DsLocalCode"                                       AS ds_local_code,
        qdcqi."DsMnem"                                            AS ds_mnem,
        qdcqi."CovergCode"                                        AS coverg_code,
        qdcqi."StatusCode"                                        AS status_code,
        qdcqi."PermID"                                            AS perm_id,
        qdcqi."TypeCode"                                          AS type_code,
        qdcqi."PrimISOCurrCode"                                   AS prim_iso_curr_code,
        qdcqi."startdate"                                         AS start_date,
        qdcqi."enddate"                                           AS end_date,
        ds."DelistDate"                                           AS delisted_date,
        qdeqi."InfoCode"                                          AS info_code,
        qdeqi."ISOCurrCode"                                       AS iso_curr_code,
        (upper(trim(qdeqi."IsPrimExchQt")) = 'Y')::boolean        AS is_primary_exchange,
        qdeqi."PriceUnit"                                         AS price_unit,
        qdeqi."StartDate"                                         AS start_date_quote,
        qdeqi."AltDsCode"                                         AS alt_ds_code,
        qdeqi."QtPermID"                                          AS qt_perm_id,
        coalesce(qdeqi."mic", upm.perm_mic)                      AS mic,
        qdeqi."MICDesc"                                           AS mic_desc,
        qde."ExchIntCode"                                         AS exch_int_code,
        qde."DsExchCode"                                          AS ds_exch_code,
        qde."ExchType"                                            AS exch_type,
        qde."ExchName"                                            AS exch_name,
        qde."ExchMnem"                                            AS exch_mnem,
        qde."ExchCtryCode"                                        AS exch_ctry_code,
        qde."CtryCodeType"                                        AS ctry_code_type,
        -- Type IDs from seeds (scalar subqueries)
        (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
        (SELECT venue_type_id FROM {{ ref('venue_type') }} WHERE mnemonic = 'EXCH') AS venue_type_id,
        (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id,
        upm.perm_lot_size,
        upm.perm_is_primary,
        upm.perm_instr_id,
        upm.perm_name,
        upm.perm_ticker                                           AS ticker,
        upm.perm_mic,
        upm.perm_quote_id,
        upm.perm_ric                                              AS ric
    FROM {{ ref('stg_qa_security_mapping') }} AS sqsm
    JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} AS qdcqi
      ON qdcqi."InfoCode" = sqsm."ds_info_code"
    JOIN {{ source('raw', 'qa_DS2Security') }} AS ds
        ON ds."DsSecCode" = sqsm.ds_sec_code  
    JOIN {{ source('raw', 'qa_DS2ExchQtInfo') }} AS qdeqi
      ON qdeqi."InfoCode" = qdcqi."InfoCode"
    JOIN {{ source('raw', 'qa_DS2Exchange') }} AS qde
      ON qde."ExchIntCode" = qdeqi."ExchIntCode"
    LEFT JOIN unique_perm_match AS upm
      ON upm."DsQuoteNumber" = qdcqi."DsCode"
     AND upm."ExchCode"      = qde."ExchMnem"

