SELECT
    a.user_id,
    a.vehicle_contract_type,
    a.vehicle_plate,
    a.vehicle_contract_id,
    CAST(a.end_timestamp AS DATE) ridesharing_last_activity
FROM {{ ref('fct_user_vehicle_usage') }} a
LEFT JOIN (
    SELECT
        user_id,
        MAX(end_timestamp) last_ridesharing_activity
    FROM {{ ref('fct_user_vehicle_usage') }}
    WHERE end_timestamp > DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY)
    GROUP BY user_id
) b ON a.end_timestamp = b.last_ridesharing_activity AND a.user_id = b.user_id
WHERE b.user_id IS NOT NULL