{{ config(materialized='view', schema='STAGING') }}

with source as (
    select * from {{ source('raw', 'taxi_zones') }}
)
select
    cast(locationid as int) as location_id,
    borough,
    zone,
    service_zone
from source
