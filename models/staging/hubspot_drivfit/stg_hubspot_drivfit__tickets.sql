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
    WHERE _fivetran_deleted = FALSE

)

select * from transformation
