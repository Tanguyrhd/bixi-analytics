with trips as (
    select * from {{ ref('stg_trips') }}
)

select
    -- dates & time dimensions
    DATE(started_at)                            as trip_date,
    started_at,
    ended_at,
    year,
    EXTRACT(MONTH from started_at)              as month,
    FORMAT_DATE('%B', DATE(started_at))         as month_name,
    EXTRACT(WEEK from started_at)               as week_of_year,
    EXTRACT(DAYOFWEEK from started_at)          as day_of_week,
    FORMAT_DATE('%A', DATE(started_at))         as day_name,
    EXTRACT(HOUR from started_at)               as hour_of_day,

    -- origin station dimensions
    start_station_name,
    start_arrondissement,
    start_lat,
    start_lng,

    -- destination station dimensions
    end_station_name,
    end_arrondissement,
    end_lat,
    end_lng,

    -- metrics
    duration_seconds,
    ROUND(duration_seconds / 60.0, 2)           as duration_minutes,
    CASE
        WHEN duration_seconds < 300  THEN 'short (< 5 min)'
        WHEN duration_seconds < 1800 THEN 'medium (5-30 min)'
        ELSE 'long (> 30 min)'
    END                                         as duration_category

from trips
