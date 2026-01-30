{{ config(
    materialized = "view",
    meta = {
        "dagster": {
            "asset_key": ["intermediate","market","int_company_market_cap_full"],
            "group": "market"
        }
    }
) }}

-- Deduplicate: multiple rkd_code (securities) can map to same company_id
-- Keep one row per (valuation_date, company_id)
WITH ranked AS (
    SELECT
        smc.val_date AS valuation_date,
        em.internal_entity_id AS company_id,
        c.currency_id,
        (smc.consol_mkt_val * 1000000)::BIGINT AS market_cap,
        (smc.consol_num_shrs * 1000)::BIGINT AS shares_outstanding,
        ROW_NUMBER() OVER (
            PARTITION BY smc.val_date, em.internal_entity_id
            ORDER BY smc.ds_ctry_is_prim_qt DESC NULLS LAST, smc.rkd_issue_id ASC NULLS LAST
        ) AS rn
    FROM {{ ref('stg_qa_company_market_cap_full') }} smc
    JOIN master.entity_mapping em
        ON em.external_entity_id = smc.external_entity_id
        AND em.data_source_id = smc.data_source_id
        AND em.entity_type_id = smc.entity_type_id
    JOIN master.currency c
        ON c.code = smc.iso_currency_code
)
SELECT
    valuation_date,
    company_id,
    currency_id,
    market_cap,
    shares_outstanding
FROM ranked
WHERE rn = 1
