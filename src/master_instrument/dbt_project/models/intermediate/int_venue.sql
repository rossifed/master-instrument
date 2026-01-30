{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_venue"],
            "group": "reference"
        }
    }
) }}

SELECT
    vm.internal_venue_id AS internal_venue_id,
    sqe.external_exchange_id AS external_venue_id,
    sqe.data_source_id,
    sqe.exch_mnem AS mnemonic,
    sqe.exch_name AS name,
    ctry.country_id,
    sqe.venue_type_id
FROM {{ ref('stg_qa_exchange') }} sqe
-- LEFT JOIN for venue mapping (new venues don't have internal_id yet)
LEFT JOIN master.venue_mapping vm 
    ON vm.external_venue_id = sqe.external_exchange_id
    AND vm.data_source_id = sqe.data_source_id
    AND vm.venue_type_id = sqe.venue_type_id
LEFT JOIN master.country ctry 
    ON ctry.code_alpha2 = sqe.exch_ctry_code
