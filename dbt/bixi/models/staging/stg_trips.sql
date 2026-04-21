{% set years = [2023, 2024, 2025] %}

with

{% for year in years %}
trips_{{ year }} as (
    select
        STARTSTATIONNAME                as start_station_name,
        STARTSTATIONARRONDISSEMENT      as start_arrondissement,
        STARTSTATIONLATITUDE            as start_lat,
        STARTSTATIONLONGITUDE           as start_lng,
        ENDSTATIONNAME                  as end_station_name,
        ENDSTATIONARRONDISSEMENT        as end_arrondissement,
        ENDSTATIONLATITUDE              as end_lat,
        ENDSTATIONLONGITUDE             as end_lng,
        DATETIME(TIMESTAMP_MILLIS(STARTTIMEMS), 'America/Montreal')   as started_at,
        DATETIME(TIMESTAMP_MILLIS(ENDTIMEMS), 'America/Montreal')     as ended_at,
        TIMESTAMP_DIFF(
            TIMESTAMP_MILLIS(ENDTIMEMS),
            TIMESTAMP_MILLIS(STARTTIMEMS),
            SECOND
        )                               as duration_seconds,
        {{ year }}                      as year
    from {{ source('raw', 'trips_' ~ year) }}
    where
        STARTTIMEMS is not null
        and ENDTIMEMS is not null
        and ENDTIMEMS > STARTTIMEMS
){{ ',' if not loop.last }}

{% endfor %}

select * from (
    {% for year in years %}
    select * from trips_{{ year }}
    {{ 'union all' if not loop.last }}
    {% endfor %}
)
