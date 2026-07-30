

with pickup_locations as (

    select distinct
        try_to_double(pickup_longitude)     as longitude,
        try_to_double(pickup_latitude)      as latitude
    from {{ source('raw', 'trips_staged') }}
    where pickup_longitude is not null
      and pickup_latitude  is not null

),

dropoff_locations as (

    select distinct
        try_to_double(dropoff_longitude)    as longitude,
        try_to_double(dropoff_latitude)     as latitude
    from {{ source('raw', 'trips_staged') }}
    where dropoff_longitude is not null
      and dropoff_latitude  is not null

),

all_locations as (

    select * from pickup_locations
    union
    select * from dropoff_locations

),

deduplicated as (

    select distinct
        longitude,
        latitude,
        -- Bucket into grid zones for grouping
        round(latitude,  2)                 as lat_bucket,
        round(longitude, 2)                 as lon_bucket
    from all_locations

)

select * from deduplicated