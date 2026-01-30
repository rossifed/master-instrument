{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_equity"],
            "group": "reference"
        }
    }
) }}

SELECT
    im.internal_instrument_id,
    sqe.external_instrument_id,
    sqe.data_source_id,
    em.internal_entity_id AS entity_id,
    sqe.external_security_id::INT AS security_id,
    sqe.ds_sec_name AS name,
    sqe.ds_qt_name AS description,
    sqe.sec_delist_date AS delisted_date,
    sqe.issue_type_code AS issue_type,
    sqe.issue_description,
    sqe.ric_root AS symbol,
    sqe.isin,
    sqe.cusip,
    sqe.sedol,
    sqe.ric,
    sqe.ticker,
    eqt.equity_type_id,
    sqe.div_unit,
    sqe.is_major_security,
    sqe.is_primary_country,
    ctry.country_id,
    sqe.split_factor,
    sqe.split_date,
    sqe.instrument_type_id,
    sqe.entity_type_id
FROM {{ ref('stg_qa_equity') }} sqe
-- INNER JOIN for required entity mapping (uses IDs from staging)
JOIN master.entity_mapping em
    ON em.external_entity_id = sqe.external_company_id
    AND em.data_source_id = sqe.data_source_id
    AND em.entity_type_id = sqe.entity_type_id
-- LEFT JOIN for optional instrument mapping (new equities don't have internal_id yet)
LEFT JOIN master.instrument_mapping im
    ON im.external_instrument_id = sqe.external_instrument_id
    AND im.data_source_id = sqe.data_source_id
    AND im.instrument_type_id = sqe.instrument_type_id
LEFT JOIN master.country ctry
    ON ctry.code_alpha2 = sqe.region
LEFT JOIN master.equity_type eqt
    ON eqt.mnemonic = sqe.type_code
