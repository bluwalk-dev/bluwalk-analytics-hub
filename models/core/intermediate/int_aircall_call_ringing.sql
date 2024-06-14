WITH call_ringing_on_agent_log AS (
    SELECT
        id,
        call_uuid,
        user_email,
        timestamp,
        loaded_at
    FROM {{ ref('stg_aircall__call_ringing_on_agent') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        user_email,
        timestamp,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_ringing_on_agent') }}
),

latest_call_ringing_on_agent_log AS (
    SELECT * FROM (
        SELECT
            id,
            call_uuid,
            user_email employee_email,
            timestamp as ringing_at,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number 
        FROM call_ringing_on_agent_log)
    WHERE __row_number = 1
)

SELECT
    call_uuid,
    employee_email,
    ringing_at
FROM latest_call_ringing_on_agent_log