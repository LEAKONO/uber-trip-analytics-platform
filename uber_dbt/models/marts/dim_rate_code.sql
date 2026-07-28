with source as (

    select * from {{ ref('stg_rate_codes') }}

),

final as (

    select
        md5(rate_code_id)                       as rate_code_key,
        rate_code_id,
        rate_code_name,
        current_timestamp()                     as dbt_updated_at

    from source

)

select * from final