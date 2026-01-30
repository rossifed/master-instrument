{{ config(
    materialized = "view",
    schema = "maintenance",
    meta = {
        "dagster": {
            "asset_key": ["maintenance", "mt_dead_tuples"],
            "group": "maintenance",
            "description": "Dead tuples statistics for raw and master schemas. High dead tuple counts indicate tables needing VACUUM."
        }
    }
) }}

-- Dead tuples monitoring for raw and master schemas
-- Dead tuples are old row versions that slow down queries and waste disk space
-- Run VACUUM ANALYZE on tables with high dead_pct or dead_tuples count

SELECT 
    schemaname,
    relname AS table_name,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    CASE 
        WHEN n_live_tup + n_dead_tup > 0 
        THEN round(n_dead_tup::numeric / (n_live_tup + n_dead_tup) * 100, 2)
        ELSE 0 
    END AS dead_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    -- Status classification
    CASE
        WHEN n_dead_tup > 1000000 OR (n_live_tup + n_dead_tup > 0 AND n_dead_tup::numeric / (n_live_tup + n_dead_tup) > 0.30)
            THEN 'CRITICAL'
        WHEN n_dead_tup > 100000 OR (n_live_tup + n_dead_tup > 0 AND n_dead_tup::numeric / (n_live_tup + n_dead_tup) > 0.10)
            THEN 'WARNING'
        ELSE 'OK'
    END AS status,
    -- Suggested action
    CASE
        WHEN n_dead_tup > 100000 OR (n_live_tup + n_dead_tup > 0 AND n_dead_tup::numeric / (n_live_tup + n_dead_tup) > 0.10)
            THEN 'VACUUM ANALYZE ' || schemaname || '.' || relname || ';'
        ELSE NULL
    END AS suggested_action
FROM pg_stat_user_tables
WHERE schemaname IN ('raw', 'master')
ORDER BY n_dead_tup DESC
