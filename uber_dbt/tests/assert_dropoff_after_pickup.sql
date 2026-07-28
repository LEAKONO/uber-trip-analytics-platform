-- Fails if any fact_trip row has dropoff before pickup
-- A passing test returns zero rows

select trip_key, pickup_datetime, dropoff_datetime
from {{ ref('fact_trip') }}
where dropoff_datetime < pickup_datetime