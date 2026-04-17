{{ config(materialized='view') }}


select 
    customer_id,
    count(trip_id) as total_trips,
    sum(fare_amount) as total_spent,
    avg(fare_amount) as avg_fare
from {{ ref('factTrips') }}
group by customer_id