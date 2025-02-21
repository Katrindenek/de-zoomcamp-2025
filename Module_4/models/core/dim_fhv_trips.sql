{{ config(materialized="table") }}

with dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    tripdata.dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    tripdata.sr_flag,
    tripdata.affiliated_base_number,
    extract(year from tripdata.pickup_datetime) as pickup_year,
    extract(month from tripdata.pickup_datetime) as pickup_month
from {{ ref("stg_fhv_tripdata") }} tripdata
inner join dim_zones pickup_zone on tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones dropoff_zone on tripdata.dropoff_locationid = dropoff_zone.locationid
