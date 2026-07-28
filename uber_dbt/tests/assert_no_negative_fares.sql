-- Fails if any fact_trip row has a negative fare_amount
-- A passing test returns zero rows

select trip_key, fare_amount
from {{ ref('fact_trip') }}
where fare_amount < 0