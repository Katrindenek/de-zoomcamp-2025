{{ config(materialized="view") }}


with
    revenues_table as (
        select
            service_type,
            pickup_year,
            pickup_quarter,
            sum(total_amount) as revenue,
            sum(sum(total_amount)) over (
                partition by service_type, pickup_quarter order by pickup_year
            ) as revenue_stacked
        from {{ ref("fact_trips") }}
        where pickup_year between 2019 and 2020
        group by service_type, pickup_year, pickup_quarter
    )
select
    service_type,
    pickup_year,
    pickup_quarter,
    (revenue_stacked - revenue) / revenue as yoy_growth
from revenues_table
order by service_type, pickup_quarter, pickup_year
