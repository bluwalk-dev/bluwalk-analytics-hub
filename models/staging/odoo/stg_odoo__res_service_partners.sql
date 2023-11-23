with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'res_service_partner') }}
),

transformation as (

    select
        
        TO_HEX(MD5(id || 'Service')) as partner_key,
        id service_partner_id,
        name partner_name,
        'Service' as partner_marketplace,
        active,
        partner_id partner_contact_id,
        res_service_partner_type_id service_partner_type_id,
        res_service_sub_type service_partner_sub_type,
        country_id partner_country_id,
        short_description,
        show_cta
        cta_submitted_button_text,
        cta_submitted_link,
        create_date,
        create_uid,
        write_date,
        write_uid

    from source
    where _fivetran_deleted IS FALSE
)

select * from transformation