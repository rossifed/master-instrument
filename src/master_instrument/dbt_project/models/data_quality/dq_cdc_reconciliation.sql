{{ config(
    materialized = "view",
    schema = "data_quality",
    meta = {
        "dagster": {
            "asset_key": ["data_quality", "dq_cdc_reconciliation"],
            "group": "data_quality",
            "description": "Shows CDC reconciliation status for incremental loads. Displays pending changes count, last loaded version, and sync status."
        }
    }
) }}

-- CDC Reconciliation Status
-- Shows pending changes to be loaded for each CDC-enabled entity

SELECT
    'market_data' AS entity,
    (SELECT COUNT(*) FROM {{ ref('int_market_data_changes') }}) AS pending_changes,
    (SELECT COALESCE(MAX(last_source_version), 0) FROM master.market_data_load) AS last_loaded_version,
    (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_DS2PrimQtPrc_Changes') }}) AS max_source_version,
    CASE 
        WHEN (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_DS2PrimQtPrc_Changes') }}) 
           > (SELECT COALESCE(MAX(last_source_version), {{ var('change_tracking_initial_version') }}) FROM master.market_data_load)
        THEN 'PENDING'
        ELSE 'UP_TO_DATE'
    END AS status

UNION ALL

SELECT
    'company_market_cap' AS entity,
    (SELECT COUNT(*) FROM {{ ref('int_company_market_cap_changes') }}) AS pending_changes,
    (SELECT COALESCE(MAX(last_source_version), 0) FROM master.company_market_cap_load) AS last_loaded_version,
    (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_DS2MktVal_Changes') }}) AS max_source_version,
    CASE 
        WHEN (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_DS2MktVal_Changes') }}) 
           > (SELECT COALESCE(MAX(last_source_version), {{ var('change_tracking_initial_version') }}) FROM master.company_market_cap_load)
        THEN 'PENDING'
        ELSE 'UP_TO_DATE'
    END AS status

UNION ALL

SELECT
    'std_financial_value' AS entity,
    (SELECT COUNT(*) FROM {{ ref('int_std_financial_value_changes') }}) AS pending_changes,
    (SELECT COALESCE(MAX(last_source_version), 0) FROM master.std_financial_value_load) AS last_loaded_version,
    (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_RKDFndStdFinVal_Changes') }}) AS max_source_version,
    CASE 
        WHEN (SELECT MAX(sys_change_version) FROM {{ source('raw', 'qa_RKDFndStdFinVal_Changes') }}) 
           > (SELECT COALESCE(MAX(last_source_version), {{ var('change_tracking_initial_version') }}) FROM master.std_financial_value_load)
        THEN 'PENDING'
        ELSE 'UP_TO_DATE'
    END AS status
