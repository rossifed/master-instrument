{{ config(
    materialized = "view",
    schema = "maintenance",
    meta = {
        "dagster": {
            "asset_key": ["maintenance", "mt_table_indexes"],
            "group": "maintenance",
            "description": "Lists all indexes on raw and master schema tables with size, type and definition. Useful for index monitoring and optimization."
        }
    }
) }}

-- List all indexes on raw and master tables for analysis and monitoring
SELECT
    ns.nspname AS schemaname,
    t.relname  AS table_name,
    i.relname  AS index_name,
    am.amname  AS index_type,
    pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
    pg_relation_size(i.oid) AS index_size_bytes,
    ix.indisunique  AS is_unique,
    ix.indisprimary AS is_primary,
    pg_get_indexdef(i.oid) AS index_definition
FROM pg_index ix
JOIN pg_class i      ON i.oid = ix.indexrelid
JOIN pg_class t      ON t.oid = ix.indrelid
JOIN pg_namespace ns ON ns.oid = t.relnamespace
JOIN pg_am am        ON am.oid = i.relam
WHERE ns.nspname IN ('raw', 'master')
ORDER BY ns.nspname, t.relname, pg_relation_size(i.oid) DESC
