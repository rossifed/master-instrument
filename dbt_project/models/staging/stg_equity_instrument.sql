{{ config(
    materialized = 'incremental',
    unique_key = 'isin',
    incremental_strategy='merge',
    meta={
        "dagster": {
            "asset_key": ["staging", "stg_equity_instrument"]
        }
    }
) }}

with source_data as (

    select distinct
        s.security_id,
        s.company_code,
        s.company_name,
        s.isin,
        s.cusip
    from {{ ref('stg_qa_instrument') }} s
    where s.isin is not null

),

final as (

    select
        md5(concat(isin)) as id,
        isin,
        isin as symbol,
        company_code,
        company_name,
        cusip,
        security_id,
        current_timestamp  as updated_at
    from source_data

)

select * from final
