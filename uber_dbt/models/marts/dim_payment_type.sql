with source as (

    select * from {{ ref('stg_payment_types') }}

),

final as (

    select
        md5(payment_type_id)                    as payment_type_key,
        payment_type_id,
        payment_type_name,
        current_timestamp()                     as dbt_updated_at

    from source

)

select * from final