{{ config(materialized='view') }}

-- Daily operational metrics

select 
    date(trip_start_time) as trip_date,
    count(*) as total_trips,
    sum(fare_amount) as total_revenue
from {{ ref('factTrips') }}
group by date(trip_start_time)