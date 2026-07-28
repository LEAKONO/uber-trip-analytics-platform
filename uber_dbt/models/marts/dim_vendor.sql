with source as (

    select * from {{ ref('stg_vendors') }}

),

final as (

    select
        md5(vendor_id)                          as vendor_key,
        vendor_id,
        vendor_name,
        current_timestamp()                     as dbt_updated_at

    from source

)

select * from final