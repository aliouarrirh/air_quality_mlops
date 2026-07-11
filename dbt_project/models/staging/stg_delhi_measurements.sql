-- Staging : nettoyage, typage, flags qualité, features temporelles

with source as (
    select * from {{ source('raw', 'delhi_measurements') }}
),

cleaned as (
    select
        location_id,
        sensor_id,
        parameter,
        value,
        unit,
        cast(datetime_utc   as timestamp) as datetime_utc,
        cast(datetime_local as timestamp) as datetime_local,
        latitude,
        longitude,
        ingested_at,

        -- Flag qualité
        case
            when value is null                              then false
            when value < 0                                  then false
            when parameter = 'pm25' and value > 999        then false
            when parameter = 'pm10' and value > 999        then false
            when parameter = 'no2'  and value > 2000       then false
            when parameter = 'so2'  and value > 2000       then false
            when parameter = 'co'   and value > 50         then false
            when parameter = 'o3'   and value > 600        then false
            else true
        end as is_valid,

        -- Features temporelles IST
        extract(hour  from cast(datetime_local as timestamp)) as hour_local,
        extract(dow   from cast(datetime_local as timestamp)) as day_of_week,
        extract(month from cast(datetime_local as timestamp)) as month,

        -- Saisons indiennes (impact fort sur la pollution à Delhi)
        case
            when extract(month from cast(datetime_local as timestamp))
                 in (12, 1, 2)    then 'winter'       -- pollution maximale
            when extract(month from cast(datetime_local as timestamp))
                 in (3, 4, 5)     then 'summer'
            when extract(month from cast(datetime_local as timestamp))
                 in (6, 7, 8, 9)  then 'monsoon'      -- pollution minimale
            else                       'post_monsoon'
        end as season

    from source
    where value is not null
)

select * from cleaned
