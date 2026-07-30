

with source as (

    select distinct
        try_to_timestamp(pickup_datetime) as pickup_datetime
    from {{ source('raw', 'trips_staged') }}
    where pickup_datetime is not null

),

enriched as (

    select
        pickup_datetime,

        -- Date parts
        date(pickup_datetime)               as trip_date,
        year(pickup_datetime)               as trip_year,
        month(pickup_datetime)              as trip_month,
        monthname(pickup_datetime)          as trip_month_name,
        day(pickup_datetime)                as trip_day,
        dayofweek(pickup_datetime)          as day_of_week_number,
        dayname(pickup_datetime)            as day_of_week_name,
        weekofyear(pickup_datetime)         as week_of_year,
        quarter(pickup_datetime)            as trip_quarter,

        -- Time parts
        hour(pickup_datetime)               as pickup_hour,
        minute(pickup_datetime)             as pickup_minute,

        -- Business time buckets
        case
            when hour(pickup_datetime) between 6  and 9  then 'Morning Rush'
            when hour(pickup_datetime) between 10 and 15 then 'Midday'
            when hour(pickup_datetime) between 16 and 19 then 'Evening Rush'
            when hour(pickup_datetime) between 20 and 23 then 'Night'
            else 'Late Night'
        end                                 as time_of_day,

        -- Weekend flag
        case
            when dayofweek(pickup_datetime) in (0, 6) then true
            else false
        end                                 as is_weekend

    from source

)

select * from enriched