{{ config(
    materialized = "view",
    schema = "staging",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","reference","stg_qa_number_of_shares"],
            "group": "reference"
        }
    }
) }}

SELECT 
    qdns."InfoCode"::TEXT AS info_code,
    qdns."EventDate" AS event_date,
    qdns."NumShrs" AS num_shrs,
    -- Type IDs from seeds (scalar subqueries)
    (SELECT instrument_type_id FROM {{ ref('instrument_type') }} WHERE mnemonic = 'EQU') AS instrument_type_id,
    (SELECT data_source_id FROM {{ ref('data_source') }} WHERE mnemonic = 'QA') AS data_source_id
FROM {{ source('raw', 'qa_DS2NumShares') }} qdns
WHERE qdns."EventDate" >= '{{ var('min_data_date') }}'::date
  AND qdns."NumShrs" IS NOT NULL
