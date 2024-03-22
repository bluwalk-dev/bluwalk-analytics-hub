WITH current_version AS (

    WITH tripDistance AS (
        SELECT 
            partner_account_uuid, 
            CAST(dropoff_timestamp AS DATE) AS date, 
            sum(trip_distance) as trip_distance
        from {{ ref('base_uber_trips') }}
        group by partner_account_uuid, date
    ), netEarnings AS (
        SELECT 
            partner_account_uuid,
            CAST(local_datetime as date) as date, 
            SUM(amount) as net_earnings
        from {{ ref('base_uber_earnings') }}
        group by partner_account_uuid, date
    )

    SELECT
        da.date,
        upa.contact_id,
        u.user_id,
        da.partner_account_uuid,
        'Uber' as partner_name,
        u.user_location,
        da.online_minutes,
        da.trip_minutes,
        NULL working_minutes,
        da.nr_trips,
        dq.acceptance_rate,
        dq.cancellation_rate,
        dq.rating_last_500_trips as rating,
        ta.trip_distance,
        po.net_earnings
    FROM {{ ref('int_uber_activity') }} da
    LEFT JOIN {{ ref('int_uber_quality') }} dq on da.date = dq.date and da.partner_account_uuid = dq.partner_account_uuid
    LEFT JOIN tripDistance ta on ta.date = da.date and ta.partner_account_uuid = da.partner_account_uuid
    LEFT JOIN netEarnings po on po.date = da.date and po.partner_account_uuid = da.partner_account_uuid
    LEFT JOIN {{ ref('dim_partners_accounts') }} upa on da.partner_account_uuid = upa.partner_account_uuid
    LEFT JOIN {{ ref('dim_users') }} u on upa.contact_id = u.contact_id

),

deprecated_version AS (
    SELECT
        a.date,
        a.contact_id,
        u.user_id,
        '' partner_account_uuid,
        'Uber' as partner_name,
        u.user_location,
        a.online_minutes,
        NULL trip_minutes,
        NULL working_minutes,
        a.nr_trips,
        a.acceptance_rate,
        a.cancellation_rate,
        a.lifetime_rating rating,
        a.trip_distance,
        NULL netEarnings
    FROM {{ ref('stg_uber__performance') }} a
    LEFT JOIN {{ ref('dim_users') }} u on a.contact_id = u.contact_id
)

SELECT * FROM current_version
UNION ALL
SELECT * FROM deprecated_version
WHERE (online_minutes > 0 AND nr_trips > 0)
ORDER BY date DESC