SELECT * FROM (
    SELECT DISTINCT
        a.user_id,
        a.contact_id,
        b.partner_id,
        b.partner_name,
        b.partner_category,
        'Work' partner_marketplace,
        c.date,
        c.year_week,
        c.year_month
    FROM {{ ref('fct_user_financial_transactions') }} a
    LEFT JOIN {{ ref('fct_user_job_orders') }} b ON a.order_id = b.job_order_id
    LEFT JOIN {{ ref('util_calendar') }} c ON a.date = c.date
    WHERE 
        a.order_type = 'Job' AND 
        extract(year from a.date) >= 2020
)
ORDER BY date DESC