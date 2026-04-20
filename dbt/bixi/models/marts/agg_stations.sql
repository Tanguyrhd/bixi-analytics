with departures as (
    select
        start_station_name          as station_name,
        start_arrondissement        as arrondissement,
        start_lat                   as lat,
        start_lng                   as lng,
        year,
        COUNT(*)                    as departures
    from {{ ref('stg_trips') }}
    where start_station_name is not null
    group by 1, 2, 3, 4, 5
),

arrivals as (
    select
        end_station_name            as station_name,
        year,
        COUNT(*)                    as arrivals
    from {{ ref('stg_trips') }}
    where end_station_name is not null
    group by 1, 2
),

joined as (
    select
        d.station_name,
        d.arrondissement,
        d.lat,
        d.lng,
        d.year,
        d.departures,
        a.arrivals,
        d.departures - a.arrivals   as net_flow,
        ROUND(
            (d.departures - a.arrivals) / d.departures * 100
        , 2)                        as net_flow_pct
    from departures d
    left join arrivals a
        on d.station_name = a.station_name
        and d.year = a.year
    where d.departures > 100
    and a.arrivals > 100
)

select * from joined
where ABS(net_flow_pct) < 500
