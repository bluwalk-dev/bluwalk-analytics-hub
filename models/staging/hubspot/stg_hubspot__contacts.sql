with

source as (
    select
        *
    from {{ source('hubspot', 'contact') }}
),

transformation as (

    SELECT
    
        CAST(id AS INT64) id,
        CAST(is_deleted AS BOOL) is_deleted,
        CAST(property_odoo_partner_id AS INT64) odoo_partner_id,
        CAST(property_odoo_user_id AS INT64) odoo_user_id,
        CAST(property_hs_calculated_phone_number AS STRING) hs_calculated_phone_number

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

SELECT * FROM transformation

