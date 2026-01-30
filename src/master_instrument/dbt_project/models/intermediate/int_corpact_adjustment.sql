{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","reference","int_corpact_adjustment"],
            "group": "reference"
        }
    }
) }}

SELECT 
    im.internal_instrument_id AS equity_id,
    stg.adj_date,
    stg.adj_type,
    stg.end_adj_date,
    stg.adj_factor,
    stg.cum_adj_factor
FROM {{ ref('stg_qa_corpact_adjustment') }} stg
-- INNER JOIN for required instrument mapping
JOIN master.instrument_mapping im 
    ON im.external_instrument_id = stg.external_instrument_id
    AND im.data_source_id = stg.data_source_id
    AND im.instrument_type_id = stg.instrument_type_id
