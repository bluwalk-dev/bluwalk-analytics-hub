{{ config(alias='dim_partners') }}

SELECT
    rsp.id as partner_id,
    rsp.name as partner_name,
    rsp.partner_id as partner_contact_id,
    ent.full_name as partner_contact_name,
    REPLACE(INITCAP(rspt.partner_type),'_',' ') as partner_stream,
    rc.name as partner_country,
    rc.code as partner_country_code
FROM {{ ref('stg_odoo__res_sales_partners') }} rsp
LEFT JOIN {{ ref('stg_odoo__res_sales_partner_types') }} rspt ON rsp.res_sales_partner_type_id = rspt.id
LEFT JOIN {{ ref('stg_odoo__res_countries') }} rc ON rsp.country_id = rc.id
LEFT JOIN {{ ref('dim_contacts') }} ent ON ent.contact_id = rsp.partner_id
ORDER BY partner_id ASC