with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'ticket') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

select * from transformation