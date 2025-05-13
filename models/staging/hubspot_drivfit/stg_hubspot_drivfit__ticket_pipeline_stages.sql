with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'ticket_pipeline_stage') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

select * from transformation