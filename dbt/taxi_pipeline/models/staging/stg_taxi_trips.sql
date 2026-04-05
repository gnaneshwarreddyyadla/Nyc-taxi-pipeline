{{ config(materialized='view', schema='STAGING') }}

with source as (
    select * from {{ source('raw', 'taxi_trips') }}
),
cleaned as (
    select
        vendorid                                         as vendor_id,
        cast(tpep_pickup_datetime as timestamp)          as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp)         as dropoff_datetime,
        passenger_count,
        trip_distance,
        cast(pulocationid as int)                        as pickup_location_id,
        cast(dolocationid as int)                        as dropoff_location_id,
        payment_type,
        cast(fare_amount as float)                       as fare_amount,
        cast(tip_amount as float)                        as tip_amount,
        cast(total_amount as float)                      as total_amount,
        cast(congestion_surcharge as float)              as congestion_surcharge,
        cast(airport_fee as float)                       as airport_fee,
        store_and_fwd_flag,
        _loaded_at
    from source
    where total_amount > 0
      and trip_distance > 0
      and passenger_count > 0
)
select * from cleaned
