with source as (

    select * from {{ ref('stg_locations') }}

),

final as (

    select
        -- Surrogate key based on coordinates
        md5(
            cast(latitude  as string) || '|' ||
            cast(longitude as string)
        )                                       as location_key,
        latitude,
        longitude,
        lat_bucket,
        lon_bucket,
        -- NYC borough approximation from coordinates
        case
            when latitude between 40.70 and 40.88
             and longitude between -74.02 and -73.93
            then 'Manhattan'
            when latitude between 40.57 and 40.74
             and longitude between -74.04 and -73.83
            then 'Brooklyn'
            when latitude between 40.68 and 40.81
             and longitude between -73.93 and -73.70
            then 'Queens'
            when latitude between 40.79 and 40.92
             and longitude between -73.95 and -73.75
            then 'Bronx'
            when latitude between 40.49 and 40.65
             and longitude between -74.26 and -74.03
            then 'Staten Island'
            when latitude between 40.49 and 41.10
             and longitude between -74.30 and -73.65
            then 'NYC Metro Area'
            else 'Outside NYC'
        end                                     as borough,
        current_timestamp()                     as dbt_updated_at

    from source

)

select * from final