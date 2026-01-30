{#
  Macro: cdc_deduplicate
  
  Purpose: Provides the CDC deduplication pattern with:
  - Scalar subquery to get the watermark from load tracking table (evaluated once)
  - deduplicated_changes CTE with DISTINCT ON to keep only latest version per unique key
  
  Parameters:
  - source_ref: The dbt ref() to the staging changes model (e.g., "stg_qa_market_data_changes")
  - load_table: The load tracking table name (e.g., "market_data_load")
  - unique_keys: List of columns that form the unique key for deduplication
  - version_column: Column with version watermark (default: "last_source_version")
  
  Usage:
  {{ cdc_deduplicate(
      source_ref="stg_qa_market_data_changes",
      load_table="market_data_load", 
      unique_keys=["info_code", "exch_int_code", "date"]
  ) }}
  
  Then reference `deduplicated_changes` in your SELECT.
  
  Note: Uses var('change_tracking_initial_version') as fallback when load table is empty.
  This prevents loading all historical data on first run.
#}

{% macro cdc_deduplicate(source_ref, load_table, unique_keys, version_column="last_source_version") %}

WITH deduplicated_changes AS (
    SELECT DISTINCT ON ({{ unique_keys | join(', ') }})
        src.*,
        (SELECT COALESCE(MAX({{ version_column }}), {{ var('change_tracking_initial_version') }}) FROM master.{{ load_table }}) AS last_loaded_version
    FROM {{ ref(source_ref) }} src
    WHERE src.sys_change_version > (
        SELECT COALESCE(MAX({{ version_column }}), {{ var('change_tracking_initial_version') }}) 
        FROM master.{{ load_table }}
    )
    ORDER BY 
        {{ unique_keys | join(', ') }},
        sys_change_version DESC
)

{% endmacro %}
