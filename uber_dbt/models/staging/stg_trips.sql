with source as (

    select * from {{ source('raw', 'trips_staged') }}

),

cleaned as (

    select
        -- Identifiers
        raw_id,
        batch_id,
        pipeline_run_id,
        ingestion_timestamp,

        -- Timestamps — cast from string to proper timestamp
        try_to_timestamp(pickup_datetime)           as pickup_datetime,
        try_to_timestamp(dropoff_datetime)          as dropoff_datetime,

        -- Derived time measures
        datediff(
            'minute',
            try_to_timestamp(pickup_datetime),
            try_to_timestamp(dropoff_datetime)
        )                                           as trip_duration_minutes,

        -- Dimension foreign keys (will join to dims in mart)
        trim(vendor_id)                             as vendor_id,
        trim(rate_code_id)                          as rate_code_id,
        trim(payment_type)                          as payment_type,
        trim(store_fwd_flag)                        as store_fwd_flag,

        -- Location coordinates — cast to float
        try_to_double(pickup_longitude)             as pickup_longitude,
        try_to_double(pickup_latitude)              as pickup_latitude,
        try_to_double(dropoff_longitude)            as dropoff_longitude,
        try_to_double(dropoff_latitude)             as dropoff_latitude,

        -- Numeric measures — cast to float
        try_to_double(passenger_count)              as passenger_count,
        try_to_double(trip_distance)                as trip_distance,
        try_to_double(fare_amount)                  as fare_amount,
        try_to_double(extra)                        as extra,
        try_to_double(mta_tax)                      as mta_tax,
        try_to_double(tip_amount)                   as tip_amount,
        try_to_double(tolls_amount)                 as tolls_amount,
        try_to_double(improvement_surcharge)        as improvement_surcharge,
        try_to_double(total_amount)                 as total_amount,

        -- Derived revenue metrics
        case
            when try_to_double(trip_distance) > 0
            then round(
                try_to_double(fare_amount) / try_to_double(trip_distance),
                2
            )
            else null
        end                                         as fare_per_mile,

        case
            when try_to_double(fare_amount) > 0
            then round(
                try_to_double(tip_amount) / try_to_double(fare_amount) * 100,
                2
            )
            else null
        end                                         as tip_percentage,

        -- Data quality flags
        case
            when try_to_double(trip_distance) = 0 then true
            else false
        end                                         as is_zero_distance,

        case
            when try_to_double(passenger_count) = 0 then true
            else false
        end                                         as is_zero_passengers,

        case
            when try_to_double(fare_amount) < 0 then true
            else false
        end                                         as has_negative_fare,

        case
            when try_to_timestamp(dropoff_datetime)
                 < try_to_timestamp(pickup_datetime)
            then true
            else false
        end                                         as has_invalid_timestamps

    from source

)

select * from cleaned