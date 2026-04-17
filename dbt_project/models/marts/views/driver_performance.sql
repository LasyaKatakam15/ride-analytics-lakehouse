{{ config(materialized='view') }}


select 
    driver_id,
    count(trip_id) as total_trips,
    sum(fare_amount) as total_earnings,
    avg(distance_km) as avg_distance
from {{ ref('factTrips') }}
group by driver_id