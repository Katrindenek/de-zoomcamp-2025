{{
    config(
        materialized='view'
    )
}}

with mart as (
  select 
    *,
    PERCENTILE_CONT(timestamp_diff(dropoff_datetime, pickup_datetime, SECOND), 0.9) OVER(PARTITION BY pickup_year, pickup_month, pickup_zone, dropoff_zone) as p90
  from
    {{ ref("dim_fhv_trips") }}
)
select
  pickup_zone,
  dropoff_zone,
  pickup_month,
  pickup_year,
  p90,
  DENSE_RANK() OVER(PARTITION BY pickup_year, pickup_month, pickup_zone ORDER BY p90 DESC) as rn
from
  mart
