with

    source as (select * from {{ source('analytics_342899811', 'events_*') }}),

    transformation as (

        SELECT
            *
        FROM source

    )

select *
from transformation
