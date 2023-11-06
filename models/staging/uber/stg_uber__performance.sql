with

source as (
    select
        *
    from {{ source('uber', 'driver_performance') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(date AS DATE) as date,
        CAST(partner_id AS INT64) AS contact_id,
        CAST(account AS STRING) AS org_account,
        CAST(name AS STRING) AS driver_name,
        CAST(gross_fares AS NUMERIC) AS gross_fares,
        CAST(hours_online*60 AS NUMERIC) AS online_minutes,
        CAST(trips AS NUMERIC) AS nr_trips,
        CAST(ratings AS NUMERIC) AS rating,
        CAST(lifetime_rating AS NUMERIC) AS lifetime_rating,
        CAST(acceptance_rate AS NUMERIC) AS acceptance_rate,
        CAST(driver_cancellation_rate AS NUMERIC) AS cancellation_rate,
        CAST(trips*distance_per_trip AS NUMERIC) AS trip_distance
    FROM source

)

select * from transformation