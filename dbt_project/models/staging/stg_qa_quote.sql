{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_quote"]
        }
    }
) }}


    SELECT  DISTINCT
        qdcqi."DsSecCode", 
        qdcqi."Region",
        qdcqi."RegCodeTypeId", 
        qdcqi."IsPrimQt", 
        qdcqi."DsQtName" ,
        qdcqi."DsLocalCode", 
        qdcqi."DsMnem", 
        qdcqi."CovergCode", 
        qdcqi."StatusCode", 
        qdcqi."PermID", 
        qdcqi."TypeCode", 
        qdcqi."PrimISOCurrCode", 
        qdcqi."DelistDate",
        qdcqi."startdate" ,         
        qdcqi."enddate" ,      
        qdeqi."InfoCode", 
        qdeqi."ISOCurrCode",       
        qdeqi."IsPrimExchQt", 
        qdeqi."PriceUnit", 
        qdeqi."StartDate",          
        qdeqi."AltDsCode", 
        qdeqi."QtPermID", 
        qdeqi."mic",                
        qdeqi."MICDesc",            
        qde."ExchIntCode",           
        qde."DsExchCode", 
        qde."ExchType", 
        qde."ExchName", 
        qde."ExchMnem", 
        qde."ExchCtryCode", 
        qde."CtryCodeType"
    FROM {{ ref('stg_qa_security_mapping') }} sqsm
    JOIN {{ source('raw', 'qa_DS2CtryQtInfo') }} qdcqi 
        ON qdcqi."DsSecCode" = sqsm."ds_sec_code"
    JOIN {{ source('raw', 'qa_DS2ExchQtInfo') }} qdeqi 
        ON qdeqi."InfoCode" = qdcqi."InfoCode"
    JOIN {{ source('raw', 'qa_DS2Exchange') }} qde 
        ON qde."ExchIntCode" = qdeqi."ExchIntCode"

