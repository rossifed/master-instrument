{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_share_outstanding"],
            "group": "reference"
        }
    }
) }}

SELECT 
    sqso.event_date AS date,
    im.internal_instrument_id AS equity_id,
    sqso.num_shrs AS number_of_shares
FROM {{ ref('stg_qa_number_of_shares') }} sqso
-- INNER JOIN for required instrument mapping
JOIN master.instrument_mapping im 
    ON im.external_instrument_id = sqso.info_code
    AND im.data_source_id = sqso.data_source_id
    AND im.instrument_type_id = sqso.instrument_type_id
