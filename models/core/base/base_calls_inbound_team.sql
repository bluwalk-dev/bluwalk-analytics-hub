WITH 
calls_ansered_by_team AS (
    SELECT DISTINCT
        call_uuid,
        team,
        MIN(answered_at) answered_at
    FROM {{ ref('base_calls_inbound_employee') }}
    WHERE answered_at IS NOT NULL
    GROUP BY call_uuid, team
), 

calls_ringing_on_team AS (
    SELECT
        call_uuid,
        team,
        MIN(ringing_at) ringing_at
    FROM {{ ref('base_calls_inbound_employee') }}
    WHERE ringing_at IS NOT NULL
    GROUP BY call_uuid, team
)  

SELECT
    a.call_uuid,
    c.direction,
    a.team,
    c.created_at,
    a.ringing_at,
    b.answered_at,
    c.ended_at,
    c.duration,
    c.number_name,
    c.recording_url
FROM calls_ringing_on_team a
LEFT JOIN calls_ansered_by_team b ON a.call_uuid = b.call_uuid AND a.team = b.team
LEFT JOIN {{ ref('base_calls') }} c ON a.call_uuid = c.call_uuid
ORDER BY created_at DESC