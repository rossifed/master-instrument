BEGIN;

WITH stg_qa_company_with_id AS (
    SELECT
    cim.internal_company_id,
    sqc.external_id,
    sqc.source,
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
    sqc.public_since,
    sqc.com_sh_hldr AS common_shareholders,
    sqc.com_sh_hldr_dt AS common_shareholders_date,
    sqc.tot_sh_out AS total_shares_outstanding,
    sqc.tot_sh_out_dt AS shares_outstanding_updated_at,
    sqc.tot_float AS total_float_shares,
    eci.currency_id   AS estimates_currency_id,
    sci.currency_id   AS statements_currency_id, 
    sqc.last_mod_fin_dt AS last_modified_financial_at,
    sqc.last_mod_other_dt AS last_modified_non_financial_at,
    sqc.latest_fin_ann_dt AS latest_annual_financial_date,
    et.entity_type_id
  FROM staging.stg_qa_company sqc
  join ref_data.entity_type et on et.mnemonic  = 'CMPY'
  LEFT JOIN ref_data.company_mapping cim 
    ON cim.external_company_id = sqc.external_id
   AND cim.source = sqc.source
  LEFT JOIN ref_data.country ctry
    ON ctry.code = sqc.iso_cntry_code
  LEFT JOIN ref_data.currency eci 
  	ON eci.code  = sqc.est_curr_code 
  LEFT JOIN ref_data.currency sci 
  	ON sci.code  = sqc.fin_stmt_curr_code 	
),
merge_entity AS (
  MERGE INTO ref_data.entity AS tgt
  USING stg_qa_company_with_id src
  ON src.internal_company_id = tgt.entity_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      entity_type_id = src.entity_type_id
  WHEN NOT MATCHED THEN
    INSERT (name, entity_type_id)
    VALUES (src.name, src.entity_type_id)
  RETURNING entity_id, src.external_id, src.source
),
enriched_entity AS (
  SELECT
    me.entity_id,
    src.* 
  FROM merge_entity me
  JOIN stg_qa_company_with_id src
    ON src.external_id = me.external_id
),
merge_company AS (
  MERGE INTO ref_data.company AS tgt
  USING enriched_entity src
  ON tgt.company_id = src.entity_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      description = src.description,
      employee_count = src.employee_count,
      employee_count_date = src.employee_count_date,
      st_address_1 = src.st_address_1,
      st_address_2 = src.st_address_2,
      city = src.city,
      state = src.state,
      postal_code = src.postal_code,
      country_id = src.country_id,
      public_since = src.public_since,
      common_shareholders = src.common_shareholders,
      common_shareholders_date = src.common_shareholders_date,
      total_shares_outstanding = src.total_shares_outstanding,
      shares_outstanding_updated_at = src.shares_outstanding_updated_at,
      total_float_shares = src.total_float_shares,
      estimates_currency_id = src.estimates_currency_id,
      statements_currency_id = src.statements_currency_id,
      last_modified_financial_at = src.last_modified_financial_at,
      last_modified_non_financial_at = src.last_modified_non_financial_at,
      latest_annual_financial_date = src.latest_annual_financial_date
  WHEN NOT MATCHED THEN
    INSERT (
      company_id, name, description,
      employee_count, employee_count_date,
      st_address_1, st_address_2, city, state, postal_code,
      country_id, public_since,
      common_shareholders, common_shareholders_date,
      total_shares_outstanding, shares_outstanding_updated_at,
      total_float_shares, estimates_currency_id, statements_currency_id,
      last_modified_financial_at, last_modified_non_financial_at,
      latest_annual_financial_date
    )
    VALUES (
      src.entity_id, src.name, src.description,
      src.employee_count, src.employee_count_date,
      src.st_address_1, src.st_address_2, src.city, src.state, src.postal_code,
      src.country_id, src.public_since,
      src.common_shareholders, src.common_shareholders_date,
      src.total_shares_outstanding, src.shares_outstanding_updated_at,
      src.total_float_shares, src.estimates_currency_id, src.statements_currency_id,
      src.last_modified_financial_at, src.last_modified_non_financial_at,
      src.latest_annual_financial_date
    )
),
update_mapping AS (
  INSERT INTO ref_data.company_mapping (
    source, external_company_id, internal_company_id
  )
  SELECT
    source, external_id, entity_id
  FROM merge_entity
  ON CONFLICT DO NOTHING
)

SELECT 1;
COMMIT;