SELECT
    a.date,
    a.contact_id,
    u.user_id,
    a.partner_account_uuid,
    'Bolt' partner_name,
    u.location,
    a.online_minutes,
    NULL trip_minutes,
    a.working_minutes,
    a.nr_trips,
    a.acceptance_rate,
    NULL as cancellation_rate,
    a.average_rating as rating,
    a.distance_driven trip_distance,
    a.gross_revenue * 0.75 estimated_net_earnings
FROM {{ ref('stg_bolt__performance') }} a
LEFT JOIN {{ ref('dim_users') }} u on a.contact_id = u.contact_id
WHERE (a.online_minutes > 0 AND a.nr_trips > 0)