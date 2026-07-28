with source as (

    select * from {{ ref('stg_datetime') }}

),

final as (

    select
        -- Surrogate key
        md5(cast(pickup_datetime as string))    as datetime_key,
        pickup_datetime,
        trip_date,
        trip_year,
        trip_quarter,
        trip_month,
        trip_month_name,
        week_of_year,
        trip_day,
        day_of_week_number,
        day_of_week_name,
        pickup_hour,
        pickup_minute,
        time_of_day,
        is_weekend,
        current_timestamp()                     as dbt_updated_at

    from source

)

select * from final