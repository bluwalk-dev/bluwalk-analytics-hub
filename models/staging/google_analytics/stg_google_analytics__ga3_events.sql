with

    source as (select * from {{ source('analytics_249934410', 'events_*') }}),

    transformation as (

        SELECT
            *
        FROM source

    )

select *
from transformation
