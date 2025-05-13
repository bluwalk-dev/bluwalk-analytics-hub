with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'deal_contact') }}
),

transformation as (

    select

        CAST (contact_id AS INT64) AS contact_id,
        CAST (deal_id AS INT64) AS deal_id,
        CAST (type_id AS INT64) AS type_id

    from source

)

select * from transformation