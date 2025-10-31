{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging", "stg_qa_instrument_id"]
        }
    }
) }}

SELECT DISTINCT ds_sec_code AS external_instrument_id, 'QA' as source

FROM {{ ref('stg_qa_equity_instrument') }}
