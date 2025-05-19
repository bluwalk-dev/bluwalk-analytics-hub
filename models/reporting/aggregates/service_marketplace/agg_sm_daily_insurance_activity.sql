WITH policies AS (
    SELECT
        ipp.start_date, 
        ipp.end_date,
        u.user_id,
        ip.partner_id AS contact_id,
        INITCAP(ipt.insurance_type_line) AS partner_name
    from {{ ref('stg_odoo__insurance_policy_payments') }} ipp
    left join {{ ref('stg_odoo__insurance_policies') }} ip on ipp.policy_id = ip.id
    left join {{ ref('dim_insurance_types') }} ipt on ip.policy_type_id = ipt.insurance_type_id
    left join {{ ref('dim_users') }} u ON ip.partner_id = u.contact_id
)

SELECT DISTINCT
    a.date,
    a.year_week,
    a.year_month,
    b.user_id,
    b.contact_id,
    'b60236a65364d02f8ff1c9b30d08d856' partner_key,
    'Service' AS partner_marketplace,
    'Insurance' partner_category,
    partner_name
FROM {{ ref('util_calendar') }} a
LEFT JOIN policies b ON a.date BETWEEN b.start_date AND b.end_date
WHERE 
    b.start_date IS NOT NULL AND 
    contact_id IS NOT NULL AND
    a.date <= CURRENT_DATE()
ORDER BY date DESC