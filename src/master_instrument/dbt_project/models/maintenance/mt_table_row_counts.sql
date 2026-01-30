{{ config(
    materialized = "view",
    schema = "maintenance",
    meta = {
        "dagster": {
            "asset_key": ["maintenance", "mt_table_row_counts"],
            "group": "maintenance",
            "description": "Shows exact row counts for all master schema tables. Dynamically discovers tables at compile time."
        }
    }
) }}

-- Dynamic exact row counts for all master schema tables
-- Uses information_schema to discover tables at compile time

{% set tables_query %}
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'master' 
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
{% endset %}

{% set results = run_query(tables_query) %}

{% if execute %}
    {% set table_list = results.columns[0].values() %}
{% else %}
    {% set table_list = [] %}
{% endif %}

{% for table_name in table_list %}
SELECT 
    'master' AS schemaname, 
    '{{ table_name }}' AS table_name, 
    COUNT(*)::BIGINT AS row_count 
FROM master.{{ table_name }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% if table_list | length == 0 %}
-- Fallback if no tables found during parsing
SELECT 'master'::TEXT AS schemaname, ''::TEXT AS table_name, 0::BIGINT AS row_count WHERE FALSE
{% endif %}

ORDER BY row_count DESC
