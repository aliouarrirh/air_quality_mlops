-- Mart horaire : pivot polluants + calcul AQI NAQI

with base as (
    select * from {{ ref('stg_delhi_measurements') }}
    where is_valid = true
),

pivoted as (
    select
        location_id,
        date_trunc('hour', datetime_utc) as hour_utc,
        hour_local,
        day_of_week,
        month,
        season,
        latitude,
        longitude,
        avg(case when parameter = 'pm25' then value end) as pm25,
        avg(case when parameter = 'pm10' then value end) as pm10,
        avg(case when parameter = 'no2'  then value end) as no2,
        avg(case when parameter = 'so2'  then value end) as so2,
        avg(case when parameter = 'co'   then value end) as co,
        avg(case when parameter = 'o3'   then value end) as o3,
        count(*) as measurement_count
    from base
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

with_aqi as (
    select
        *,
        -- AQI NAQI basé sur PM2.5 (standard indien)
        case
            when pm25 is null  then null
            when pm25 <= 30    then round((pm25 / 30.0) * 50)
            when pm25 <= 60    then round(50  + ((pm25 - 30)  / 30.0)  * 50)
            when pm25 <= 90    then round(100 + ((pm25 - 60)  / 30.0)  * 100)
            when pm25 <= 120   then round(200 + ((pm25 - 90)  / 30.0)  * 100)
            when pm25 <= 250   then round(300 + ((pm25 - 120) / 130.0) * 100)
            else 400
        end as aqi_pm25,

        case
            when pm25 <= 30  then 'Good'
            when pm25 <= 60  then 'Satisfactory'
            when pm25 <= 90  then 'Moderate'
            when pm25 <= 120 then 'Poor'
            when pm25 <= 250 then 'Very Poor'
            else 'Severe'
        end as aqi_category

    from pivoted
)

select * from with_aqi
