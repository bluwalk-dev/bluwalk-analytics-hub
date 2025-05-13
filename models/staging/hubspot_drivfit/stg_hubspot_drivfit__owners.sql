with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'owner') }}
),

transformation as (

    select
        
        CAST (owner_id AS INT64) AS owner_id,
        CAST (active_user_id AS INT64) AS active_user_id,
        CAST (created_at AS TIMESTAMP) AS created_at,
        CAST (email AS STRING) AS email,
        CAST (first_name AS STRING) AS first_name,
        CAST (is_active AS BOOL) AS is_active,
        CAST (last_name AS STRING) AS last_name,
        CAST (updated_at AS TIMESTAMP) AS updated_at,

    from source

)

select * from transformation