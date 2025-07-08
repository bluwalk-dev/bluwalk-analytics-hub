with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'ticket') }}
),

transformation as (

    SELECT
    
        * EXCEPT(_fivetran_deleted, _fivetran_synced)

    FROM source

)

select * from transformation
