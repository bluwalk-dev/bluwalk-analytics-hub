with

source as (
    select
        *
    from {{ source('marts', 'rpt_vehicle_current_status') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

SELECT * FROM transformation