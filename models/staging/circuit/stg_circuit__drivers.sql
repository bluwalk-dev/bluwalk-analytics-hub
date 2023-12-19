with

    source as (select * from {{ source("circuit", "drivers") }}),

    transformation as (

        select
            REPLACE(id, 'drivers/', '') driver_id,
            name driver_name,
            email driver_email,
            phone driver_phone,
            active driver_active,
            TIMESTAMP_MILLIS(load_timestamp) load_timestamp
        from source

    )

select *
from transformation
