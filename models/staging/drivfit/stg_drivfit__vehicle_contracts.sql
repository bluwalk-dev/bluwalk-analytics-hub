with

source as (
    select
        *
    from {{ source('marts', 'rpt_bluwalk_vehicle_contracts') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

SELECT * FROM transformation