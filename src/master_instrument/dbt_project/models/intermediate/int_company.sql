{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_company"],
            "group": "reference"
        }
    }
) }}

SELECT
    em.internal_entity_id AS internal_entity_id,
    sqc.external_company_id AS external_entity_id,
    sqc.data_source_id,
    sqc.primary_name AS name,
    sqc.description,
    sqc.employees AS employee_count,
    sqc.emp_last_upd_dt AS employee_count_date,
    sqc.st_add_1 AS st_address_1,
    sqc.st_add_2 AS st_address_2,
    sqc.city,
    sqc.state,
    sqc.post AS postal_code,
    ctry.country_id,
    sqc.coa_type_code AS industry_code,
    sqc.public_since,
    sqc.com_sh_hldr AS common_shareholders,
    sqc.com_sh_hldr_dt AS common_shareholders_date,
    sqc.tot_sh_out AS total_shares_outstanding,
    sqc.tot_sh_out_dt AS shares_outstanding_updated_at,
    sqc.tot_float AS total_float_shares,
    eci.currency_id AS estimates_currency_id,
    sci.currency_id AS statements_currency_id, 
    sqc.last_mod_fin_dt AS last_modified_financial_at,
    sqc.last_mod_other_dt AS last_modified_non_financial_at,
    sqc.latest_fin_ann_dt AS latest_annual_financial_date,
    sqc.organization_id::INT,
    sqc.entity_type_id,
    sqc.phone,
    uoem.internal_entity_id AS ultimate_organization_id,
    pcem.internal_entity_id AS primary_company_id
FROM {{ ref('stg_qa_company') }} sqc
-- LEFT JOIN for entity mapping (new companies don't have internal_id yet)
LEFT JOIN master.entity_mapping em 
    ON em.external_entity_id = sqc.external_company_id
    AND em.data_source_id = sqc.data_source_id
    AND em.entity_type_id = sqc.entity_type_id
LEFT JOIN master.country ctry
    ON ctry.code_alpha3 = sqc.iso_cntry_code
LEFT JOIN master.currency eci 
    ON eci.code = sqc.est_curr_code 
LEFT JOIN master.currency sci 
    ON sci.code = sqc.fin_stmt_curr_code 	
-- Self-reference mappings for ultimate_organization and primary_company
LEFT JOIN master.entity_mapping uoem 
    ON uoem.external_entity_id = sqc.ultimate_organization_code
    AND uoem.data_source_id = sqc.data_source_id
    AND uoem.entity_type_id = sqc.entity_type_id
LEFT JOIN master.entity_mapping pcem 
    ON pcem.external_entity_id = sqc.rkd_rel_to_code
    AND pcem.data_source_id = sqc.data_source_id
    AND pcem.entity_type_id = sqc.entity_type_id
