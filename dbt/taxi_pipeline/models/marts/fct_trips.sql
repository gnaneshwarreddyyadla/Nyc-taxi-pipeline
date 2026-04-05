with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),
zones as (
    select * from {{ ref('stg_taxi_zones') }}
)
select
    t.pickup_datetime,
    t.dropoff_datetime,
    datediff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration_min,
    t.trip_distance,
    t.passenger_count,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.payment_type,
    pu.borough  as pickup_borough,
    pu.zone     as pickup_zone,
    do.borough  as dropoff_borough,
    do.zone     as dropoff_zone
from trips t
left join zones pu on t.pickup_location_id  = pu.location_id
left join zones do on t.dropoff_location_id = do.location_id
