WITH call_answered_log AS (
    SELECT
        id,
        call_uuid,
        user_email,
        timestamp,
        loaded_at
    FROM {{ ref('stg_aircall__call_answered') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        user_email,
        timestamp,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_answered') }}
),

merged_calls AS (
    SELECT * EXCEPT (__row_number) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY call_uuid ORDER BY loaded_at DESC) AS __row_number 
        FROM call_answered_log)
    WHERE __row_number = 1
)

SELECT
    call_uuid,
    user_email employee_email,
    timestamp as answered_at
FROM merged_calls