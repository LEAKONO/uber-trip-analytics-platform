with trips as (

    select * from {{ ref('stg_trips') }}

),

dim_datetime as (

    select * from {{ ref('dim_datetime') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

dim_vendor as (

    select * from {{ ref('dim_vendor') }}

),

dim_rate_code as (

    select * from {{ ref('dim_rate_code') }}

),

dim_payment_type as (

    select * from {{ ref('dim_payment_type') }}

),

-- Filter out bad data — send to quarantine conceptually
valid_trips as (

    select * from trips
    where not is_zero_distance
      and not is_zero_passengers
      and not has_negative_fare
      and not has_invalid_timestamps
      and trip_duration_minutes > 0
      and trip_duration_minutes < 300  -- cap at 5 hours (outlier removal)

),

final as (

    select
        -- Surrogate key
        md5(
            t.raw_id
        )                                           as trip_key,

        -- Foreign keys to dimensions
        md5(cast(t.pickup_datetime as string))      as datetime_key,
        md5(
            cast(t.pickup_latitude  as string) || '|' ||
            cast(t.pickup_longitude as string)
        )                                           as pickup_location_key,
        md5(
            cast(t.dropoff_latitude  as string) || '|' ||
            cast(t.dropoff_longitude as string)
        )                                           as dropoff_location_key,
        md5(t.vendor_id)                            as vendor_key,
        md5(t.rate_code_id)                         as rate_code_key,
        md5(t.payment_type)                         as payment_type_key,

        -- Timestamps
        t.pickup_datetime,
        t.dropoff_datetime,

        -- Trip measures
        t.passenger_count,
        t.trip_distance,
        t.trip_duration_minutes,
        t.fare_per_mile,
        t.tip_percentage,

        -- Revenue measures
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,

        -- Degenerate dimensions
        t.store_fwd_flag,

        -- Pipeline metadata
        t.raw_id,
        t.batch_id,
        t.ingestion_timestamp,
        current_timestamp()                         as dbt_updated_at

    from valid_trips t

    left join dim_vendor v
        on t.vendor_id = v.vendor_id

    left join dim_rate_code r
        on t.rate_code_id = r.rate_code_id

    left join dim_payment_type p
        on t.payment_type = p.payment_type_id

)

select * from final