SELECT
    user_id,
    contact_id,
    partner_id,
    partner_name,
    partner_stream,
    min(date) as date,
    min(year_week) as year_week,
    min(year_month) as year_month
FROM {{ ref('fct_user_activity') }}
GROUP BY user_id, contact_id, partner_id, partner_name, partner_stream
ORDER BY date DESC