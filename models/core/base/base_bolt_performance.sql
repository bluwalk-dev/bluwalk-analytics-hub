WITH driver_engagement AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY partner_account_uuid, date 
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_bolt__driver_engagement") }} 
        )
    WHERE __row_number = 1
)

SELECT * FROM (
    SELECT
        a.date,
        a.contact_id,
        u.user_id,
        a.partner_account_uuid,
        'Bolt' partner_name,
        u.user_location,
        a.online_minutes,
        NULL trip_minutes,
        a.working_minutes,
        a.nr_trips,
        a.acceptance_rate,
        NULL as cancellation_rate,
        ROUND(a.average_rating, 2) as rating,
        a.distance_driven trip_distance,
        a.gross_revenue * 0.75 estimated_net_earnings
    FROM {{ ref('stg_bolt__performance') }} a
    LEFT JOIN {{ ref('dim_users') }} u on a.contact_id = u.contact_id
    WHERE 
        a.online_minutes > 0 AND 
        a.nr_trips > 0 AND
        a.date <= '2023-11-05'

    UNION ALL

    SELECT
        a.date,
        b.contact_id,
        b.user_id,
        a.partner_account_uuid,
        'Bolt' partner_name,
        u.user_location,
        a.online_minutes,
        NULL trip_minutes,
        NULL working_minutes,
        a.completed_orders nr_trips,
        a.acceptance_rate,
        NULL as cancellation_rate,
        ROUND(a.average_driver_rating, 2) as rating,
        ROUND((a.average_ride_distance_meters *  a.completed_orders / 1000), 2)  trip_distance,
        ROUND(c.net_earnings, 2) net_earnings
    FROM driver_engagement a
    LEFT JOIN {{ ref('dim_partners_accounts') }} b ON a.partner_account_uuid = b.partner_account_uuid
    LEFT JOIN {{ ref('dim_users') }} u on b.contact_id = u.contact_id
    LEFT JOIN {{ ref('base_bolt_earnings') }} c ON a.partner_account_uuid = c.partner_account_uuid AND a.date = c.date
)
ORDER BY date DESC