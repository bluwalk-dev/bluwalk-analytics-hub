SELECT * FROM (
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
    WHERE 
        a.online_minutes > 0 AND 
        a.nr_trips > 0 /*AND
        a.date <= '2023-11-05'

    UNION ALL

    SELECT
        a.date,
        b.contact_id,
        b.user_id,
        a.partner_account_uuid,
        'Bolt' partner_name,
        u.location,
        a.online_minutes,
        NULL trip_minutes,
        NULL working_minutes,
        a.completed_orders nr_trips,
        a.acceptance_rate,
        NULL as cancellation_rate,
        a.average_driver_rating as rating,
        a.average_ride_distance_meters *  a.completed_orders trip_distance,
        c.net_earnings
    FROM {{ ref('stg_bolt__drivers_engagement') }} a
    LEFT JOIN {{ ref('dim_partners_accounts') }} b ON a.partner_account_uuid = b.partner_account_uuid
    LEFT JOIN {{ ref('dim_users') }} u on b.contact_id = u.contact_id
    LEFT JOIN {{ ref('stg_bolt__earnings') }} c ON a.partner_account_uuid = c.partner_account_uuid AND a.date = c.date*/
)
ORDER BY date DESC