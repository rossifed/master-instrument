{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_total_return_full"],
            "group": "market"
        }
    }
) }}

SELECT
    str.date AS value_date,
    qm.internal_quote_id AS quote_id,
    str.return_index AS value
FROM {{ ref('stg_qa_total_return_full') }} str
JOIN master.quote_mapping qm
    ON qm.external_quote_id = str.external_quote_id
    AND qm.data_source_id = str.data_source_id
