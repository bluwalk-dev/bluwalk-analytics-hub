with

    source as (select * from {{ source('analytics_323684004', 'events_*') }}),

    transformation as (

        SELECT
            *
        FROM source

    )

select *
from transformation
