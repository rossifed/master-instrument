{{ config(
    materialized = "view",
    schema = "master",
    meta = {
        "dagster": {
            "asset_key": ["master", "views", "vw_company"],
            "group": "reference"
        }
    }
) }}

SELECT 
    c.company_id,
    e.name,
    e.description,
    c.employee_count,
    c.employee_count_date,
    c.st_address_1,
    c.st_address_2,
    c.city,
    c.state,
    c.postal_code,
    c.country_id,
    c.public_since,
    c.common_shareholders,
    c.common_shareholders_date,
    c.total_shares_outstanding,
    c.shares_outstanding_updated_at,
    c.total_float_shares,
    estccy.code AS estimates_ccy,
    statccy.code AS statements_ccy,
    c.ultimate_organization_id,
    ultimate.name AS ultimate_organization_name,
    c.primary_company_id,
    prim.name AS primary_company_name,
    c.last_modified_financial_at,
    c.last_modified_non_financial_at,
    c.latest_annual_financial_date,
    c.latest_interim_financial_date,
    c.organization_id,
    c.phone,
    cw.url AS website
FROM {{ source('master', 'company') }} c
JOIN {{ source('master', 'entity') }} e 
    ON e.entity_id = c.company_id
LEFT JOIN {{ source('master', 'classification_scheme') }} cs 
    ON cs.mnemonic = 'GICS'
LEFT JOIN {{ source('master', 'entity_classification') }} ec 
    ON ec.entity_id = e.entity_id 
    AND ec.classification_scheme_id = cs.classification_scheme_id
LEFT JOIN {{ source('master', 'classification_node') }} cn 
    ON cn.classification_scheme_id = cs.classification_scheme_id  
    AND cn.code = ec.classification_node_code
LEFT JOIN {{ source('master', 'currency') }} estccy 
    ON estccy.currency_id = c.estimates_currency_id
LEFT JOIN {{ source('master', 'currency') }} statccy 
    ON statccy.currency_id = c.statements_currency_id
LEFT JOIN {{ source('master', 'company_weblink') }} cw 
    ON cw.company_id = c.company_id 
    AND cw.weblink_type_id = 1
LEFT JOIN {{ source('master', 'entity') }} ultimate 
    ON ultimate.entity_id = c.ultimate_organization_id
LEFT JOIN {{ source('master', 'entity') }} prim 
    ON prim.entity_id = c.primary_company_id
