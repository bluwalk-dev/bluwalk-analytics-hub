with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'contact_company') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

select * from transformation