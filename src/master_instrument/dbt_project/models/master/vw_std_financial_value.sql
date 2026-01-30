{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["master","views", "vw_std_financial_value"],
            "group": "fundamental"
        }
    }
) }}

-- Simplified view using denormalized columns in std_financial_value
-- Avoids joining through filing for commonly needed fields

SELECT 
    -- Company (from denormalized column)
    sfv.company_id,
    e.name AS company_name,
    
    -- Item
    sfv.std_financial_item_id AS item_id,
    sfi.name AS item_name,
    
    -- Period (from denormalized columns)
    sfv.period_type_id,
    fpt.name AS period_type,
    fpt.months AS period_months,
    fpt.mnemonic AS period_mnemonic,
    sfv.period_end_date,
    sfv.calendar_end_date,
    sfv.filing_end_date,
    
    -- Statement type (from denormalized column)
    fst.financial_statement_type_id as statement_type_id,
    fst.mnemonic AS statement_type_mnemonic,
    fst.name AS statement_type_name,
    
    -- Value
    sfv.value,
    
    -- Statement details (still need join)
    sfs.std_financial_statement_id AS statement_id,
    sfs.is_complete,
    sfs.is_consolidated,
    sfs.public_date,
    sfs.last_update_date,
    
    -- Filing details (still need join for these)
    sff.std_financial_filing_id,
    sff.fiscal_year,
    sff.fiscal_month,
    sff.is_final,
    sff.is_interim,
    sff.converted_currency_id,
    cccy.code AS converted_currency_code,
    sff.reported_currency_id,
    rccy.code AS reported_currency_code,
    sff.converted_unit,
    sff.reported_unit

FROM {{ source('master', 'std_financial_value') }} sfv
-- Direct joins using denormalized FK columns
JOIN {{ source('master', 'entity') }} e 
    ON e.entity_id = sfv.company_id
JOIN {{ source('master', 'std_financial_item') }} sfi 
    ON sfi.std_financial_item_id = sfv.std_financial_item_id
JOIN {{ source('master', 'financial_period_type') }} fpt 
    ON fpt.financial_period_type_id = sfv.period_type_id
JOIN {{ source('master', 'financial_statement_type') }} fst 
    ON fst.financial_statement_type_id = sfv.statement_type_id
-- Join to statement/filing only for non-denormalized fields
JOIN {{ source('master', 'std_financial_statement') }} sfs 
    ON sfs.std_financial_statement_id = sfv.std_financial_statement_id
JOIN {{ source('master', 'std_financial_filing') }} sff 
    ON sff.std_financial_filing_id = sfs.std_financial_filing_id
JOIN {{ source('master', 'currency') }} cccy 
    ON cccy.currency_id = sff.converted_currency_id
JOIN {{ source('master', 'currency') }} rccy 
    ON rccy.currency_id = sff.reported_currency_id
