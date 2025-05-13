with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'users') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

select * from transformation