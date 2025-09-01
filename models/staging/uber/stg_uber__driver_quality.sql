with

source as (
    select
        *
    from {{ source('uber_v2', 'src_uber_driver_quality') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(driver_uuid AS STRING) as partner_account_uuid,
        CAST(first_name AS STRING) as first_name,
        CAST(last_name AS STRING) as last_name,
        CAST(nr_trips AS INT) as nr_trips,
        CAST(acceptance_rate AS NUMERIC) as acceptance_rate,
        CAST(cancellation_rate AS NUMERIC) as cancellation_rate,
        CAST(conclusion_rate AS NUMERIC) as conclusion_rate,
        CAST(rating_4weeks AS NUMERIC) as rating_4_weeks,
        CAST(rating_500trips AS NUMERIC) as rating_last_500_trips,
        CAST(date AS DATE) as date
    FROM source

)

select * from bluwalk-analytics-hub.staging.stg_uber_driver_quality