WITH
service_pipelines AS (
    SELECT 'DF |  Agency' AS number_name, 'Customer Service' AS team, 'Drivfit' as brand
    UNION ALL
    SELECT 'DF | Administrative', 'Operations', 'Drivfit'
    UNION ALL
    SELECT 'DF | Avarias ou sinistros', 'Customer Service', 'Drivfit'
    UNION ALL
    SELECT 'DF | Bookings', 'Customer Service', 'Drivfit'
    UNION ALL
    SELECT 'DF | Logistics', 'Operations', 'Drivfit'
    UNION ALL
    SELECT 'DF | Repair', 'Operations', 'Drivfit'
    UNION ALL
    SELECT 'DF | Operations [IVR]', 'Customer Service', 'Drivfit'
    UNION ALL
    SELECT 'DF | Support', 'Customer Service', 'Drivfit'
    UNION ALL
    SELECT 'DF | Sales [Whatsapp]', 'Sales', 'Drivfit'
    UNION ALL
    SELECT 'DF | Main Line [PORT] [IVR] [Whatsapp]', 'Customer Service', 'Drivfit'
    UNION ALL
    SELECT 'BW | Insurance', 'Insurance', 'Bluwalk'
    UNION ALL
    SELECT 'BW | Customer Activation', 'Customer Service', 'Bluwalk'
    UNION ALL
    SELECT 'BW | Main Number', 'Customer Service', 'Bluwalk'
    UNION ALL
    SELECT 'BW | Customer Service', 'Customer Service', 'Bluwalk'
    UNION ALL
    SELECT 'Customer Service', 'Customer Service', 'Bluwalk'
    UNION ALL
    SELECT 'Main Number', 'Customer Service', 'Bluwalk'
    UNION ALL
    SELECT 'Insurance', 'Insurance', 'Bluwalk'
)

SELECT
    b.team,
    b.brand,
    a.*
FROM (
    SELECT
        a.id,
        a.number_id,
        TIMESTAMP_SECONDS(a.started_at) as started_ts,
        DATETIME(TIMESTAMP_SECONDS(a.started_at), 'Europe/Lisbon') as started_local,
        TIMESTAMP_SECONDS(a.answered_at) as answered_ts,
        DATETIME(TIMESTAMP_SECONDS(a.answered_at), 'Europe/Lisbon') as answered_local,
        TIMESTAMP_SECONDS(a.ended_at) as ended_ts,
        DATETIME(TIMESTAMP_SECONDS(a.ended_at), 'Europe/Lisbon') as ended_local,
        a.direction,
        b.name as number_name,
        a.missed_call_reason,
        CASE
            WHEN missed_call_reason IN ('agents_did_not_answer', 'no_available_agent') THEN true
            WHEN missed_call_reason IS NULL THEN true
            ELSE false
        END valid_call,
        a.raw_digits number_from,
        a.recording,
        a.duration as duration_sec,
        a.assigned_to,
        a.user_id,
        a.transferred_by,
        a.transferred_to,
        a.archived,
        a.asset
    FROM {{ ref('stg_aircallV3__calls') }} a
    LEFT JOIN {{ ref('stg_aircallV3__numbers') }} b ON a.number_id = b.id

    UNION ALL

    SELECT * 
    FROM {{ ref('stg_aircall_legacy_calls') }}
) a
LEFT JOIN service_pipelines b on a.number_name = b.number_name
WHERE team IS NOT NULL
ORDER BY started_local ASC