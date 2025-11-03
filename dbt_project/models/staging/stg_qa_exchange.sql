{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_exchange"]
        }
    }
) }}


    SELECT  DISTINCT
        sqq."ExchIntCode"::TEXT as external_exchange_id,          
        sqq."ExchIntCode",           
        sqq."DsExchCode", 
        sqq."ExchType", 
        sqq."ExchName", 
        sqq."ExchMnem", 
        sqq."ExchCtryCode", 
        sqq."CtryCodeType"
    FROM {{ ref('stg_qa_quote') }} sqq 
   

