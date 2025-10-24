{{ config(
    materialized = 'incremental',
    schema = 'referential',
    unique_key = 'symbol',
    incremental_strategy = 'merge',
    on_schema_change = 'ignore'
) }}

with source_data as (
    select
        symbol,
        company_name as name,
        1 as instrument_type_id
    from {{ ref('stg_equity_instrument') }}
)

select * from source_data
