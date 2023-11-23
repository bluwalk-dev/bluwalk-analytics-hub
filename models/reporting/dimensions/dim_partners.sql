
WITH work_partners AS (
    SELECT
        partner_key,
        a.active partner_active,
        a.sales_partner_id,
        NULL AS service_partner_id,
        a.partner_name,
        a.partner_contact_id,
        b.name as partner_category,
        '' AS partner_sub_category,
        'Subscription' as partner_format,
        partner_marketplace,
        partner_country_id
    FROM {{ ref('stg_odoo__res_sales_partners') }} a
    LEFT JOIN {{ ref('stg_odoo__res_sales_partner_types') }} b ON a.sales_partner_type_id = b.id
    
), service_partners AS (
    SELECT
        partner_key,
        a.active partner_active,
        NULL as sales_partner_id,
        a.service_partner_id,
        a.partner_name,
        a.partner_contact_id,
        b.name as partner_category,
        a.service_partner_sub_type as partner_sub_category,
        CASE
            WHEN  REPLACE(INITCAP(b.partner_type),'_',' ') = 'Training' THEN 'One-Time'
            ELSE 'Subscription' 
        END partner_format,
        partner_marketplace,
        partner_country_id
    FROM {{ ref('stg_odoo__res_service_partners') }} a
    LEFT JOIN {{ ref('stg_odoo__res_service_partner_types') }} b ON a.service_partner_type_id = b.id
)

SELECT
    partner_key,
    partner_name,
    partner_marketplace,
    partner_category,
    partner_sub_category,
    partner_active,
    sales_partner_id,
    service_partner_id,
    partner_contact_id,
    c.contact_full_name as partner_contact_name,
    b.country_name as partner_country,
    b.country_code as partner_country_code
FROM (
    SELECT * FROM work_partners
    UNION ALL
    SELECT * FROM service_partners
) a
LEFT JOIN {{ ref('dim_countries') }} b ON a.partner_country_id = b.country_id
LEFT JOIN {{ ref('dim_contacts') }} c ON c.contact_id = a.partner_contact_id