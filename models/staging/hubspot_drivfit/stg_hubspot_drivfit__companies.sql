with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'company') }}
),

transformation as (

    SELECT
    
        *

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

select * from transformation