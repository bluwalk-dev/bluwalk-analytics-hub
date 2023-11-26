with

source as (
    SELECT *
    FROM {{ source('odoo_static', 'product_category') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation