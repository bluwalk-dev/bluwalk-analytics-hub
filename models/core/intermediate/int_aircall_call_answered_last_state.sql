WITH call_answered_log AS (
    SELECT
        id,
        call_uuid,
        user_email,
        loaded_at
    FROM {{ ref('stg_aircall__call_answered') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        user_email,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_answered') }}
)

SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number 
    FROM call_answered_log)
WHERE __row_number = 1