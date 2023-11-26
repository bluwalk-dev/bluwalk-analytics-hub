with

source as (
    select
        *
    from {{ source('odoo_static', 'res_sales_partner') }}
),

transformation as (

    select
        
        TO_HEX(MD5(id || 'Work')) as partner_key,
        id sales_partner_id,
        name partner_name,
        'Work' as partner_marketplace,
        active,
        partner_id partner_contact_id,
        res_sales_partner_type_id sales_partner_type_id,
        country_id partner_country_id,
        short_description,
        cta_submitted_button_text,
        cta_submitted_link,
        get_started_url,
        has_account_transfer,
        media1,
        media2,
        need_car_rental,
        submitted_message,
        use_own_vehicle,
        create_date,
        create_uid,
        write_date,
        write_uid

    from source
    
)

select * from transformation