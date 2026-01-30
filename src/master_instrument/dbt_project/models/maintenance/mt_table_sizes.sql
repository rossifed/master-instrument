{{ config(
    materialized = "view",
    schema = "maintenance",
    meta = {
        "dagster": {
            "asset_key": ["maintenance", "mt_table_sizes"],
            "group": "maintenance",
            "description": "Table and index sizes for raw and master schemas. Useful for monitoring disk usage and growth."
        }
    }
) }}

-- Table sizes with index sizes for raw and master schemas
-- Helps identify large tables and potential disk space issues

SELECT
    ns.nspname AS schemaname,
    c.relname AS table_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
    pg_total_relation_size(c.oid) AS total_size_bytes,
    pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
    pg_relation_size(c.oid) AS table_size_bytes,
    pg_size_pretty(pg_indexes_size(c.oid)) AS indexes_size,
    pg_indexes_size(c.oid) AS indexes_size_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid)) AS toast_size,
    -- Row estimate from pg_stat
    COALESCE(s.n_live_tup, 0) AS estimated_rows,
    -- Average row size
    CASE 
        WHEN COALESCE(s.n_live_tup, 0) > 0 
        THEN pg_size_pretty((pg_relation_size(c.oid) / s.n_live_tup)::bigint)
        ELSE '-'
    END AS avg_row_size
FROM pg_class c
JOIN pg_namespace ns ON ns.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE ns.nspname IN ('raw', 'master')
  AND c.relkind = 'r'  -- regular tables only
ORDER BY pg_total_relation_size(c.oid) DESC
