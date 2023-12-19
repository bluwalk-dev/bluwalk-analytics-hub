WITH

    source as (SELECT * FROM {{ source("circuit", "plans") }}),

    transformation AS (

        SELECT
            REPLACE(plan_id, 'plans/', '') plan_id,
            plan_title,
            plan_starts,
            REPLACE(plan_depot, 'depots/', '') plan_depot,
            plan_distributed,
            plan_writable,
            plan_optimization,
            REPLACE(driver_id, 'drivers/', '') driver_id,
            driver_name,
            driver_email,
            driver_phone,
            driver_active,
            REPLACE(route_id, 'routes/', '') route_id,
            TIMESTAMP_MILLIS(load_timestamp) load_timestamp
        FROM source
    )

SELECT *
FROM transformation
