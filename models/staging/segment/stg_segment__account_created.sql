with

source as (
    select
        *
    from {{ source('segment', 'account_created') }} 
),

transformation as (

    SELECT

        *

    FROM source

)

SELECT * FROM transformation