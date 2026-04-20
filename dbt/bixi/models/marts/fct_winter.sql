with winter as (
    select * from {{ ref('fct_trips') }}
    where month in (12, 1, 2, 3)
    and case when month = 12 then year + 1 else year end in (2024, 2025)
)

select
    -- winter season dimensions
    case when month = 12 then year + 1 else year end   as winter_year,
    case month
        when 12 then 1
        when 1  then 2
        when 2  then 3
        when 3  then 4
    end                                                 as winter_month_order,
    case month
        when 12 then 'December'
        when 1  then 'January'
        when 2  then 'February'
        when 3  then 'March'
    end                                                 as winter_month_name,

    -- reuse all dimensions from fct_trips
    trip_date,
    started_at,
    hour_of_day,
    start_station_name,
    start_arrondissement,
    start_lat,
    start_lng,
    end_station_name,
    end_arrondissement,
    end_lat,
    end_lng,
    duration_seconds,
    duration_minutes

from winter
