{{ config(
    materialized='incremental',
    unique_key='isin',
    incremental_strategy='merge',
    meta={
        "dagster": {"asset_key": ["staging", "stg_equity"]}
    }
) }}

with joined as (
    select
        i.isin,
        i.id as instrument_id,
        q.company_code,
        q.company_name,
        q.cusip,
        current_timestamp as updated_at
    from {{ ref('stg_equity_instrument') }} i
    left join {{ ref('stg_qa_instrument') }} q
        on i.isin = q.isin
)

select * from joined
