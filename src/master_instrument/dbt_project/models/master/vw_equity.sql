{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_equity"],
            "group": "reference"
        }
    }
) }}

SELECT 
    i.instrument_id,
    ent.entity_id as company_id,
    i.name,
    i.symbol,
    i.description,
    ent.name AS company_name,
    ent.description AS company_description,
    it.name AS instrument_type_description,
    it.mnemonic AS instrument_type_code,
    e.security_id,
    e.isin,
    e.cusip,
    e.sedol,
    e.ric,
    e.ticker,
    e.delisted_date,
    et.mnemonic AS equity_type_code,
    et.description AS equity_type_description,
    e.issue_type,
    e.issue_description,
    e.div_unit,
    e.is_major_security,
    c.code_alpha2 AS country_code,
    e.split_date,
    e.split_factor
FROM {{ source('master', 'equity') }} e
JOIN {{ source('master', 'instrument') }} i 
    ON i.instrument_id = e.equity_id
JOIN {{ source('master', 'instrument_type') }} it  
    ON it.instrument_type_id = i.instrument_type_id
JOIN {{ source('master', 'equity_type') }} et  
    ON et.equity_type_id = e.equity_type_id
LEFT JOIN {{ source('master', 'country') }} c 
    ON c.country_id = e.country_id
JOIN {{ source('master', 'entity') }} ent 
    ON ent.entity_id = i.entity_id
    
