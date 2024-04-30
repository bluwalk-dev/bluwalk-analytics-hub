with

source as (
    select
        *
    from {{ source('bluwalk_website_prod', 'account_created') }} 
),

transformation as (

    SELECT

        *

    FROM source

)

SELECT * FROM transformation