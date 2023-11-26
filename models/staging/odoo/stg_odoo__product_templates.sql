with

source as (
    SELECT *
    FROM {{ source('odoo_static', 'product_template') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation