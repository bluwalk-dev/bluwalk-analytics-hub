WITH call_created_log AS (
    SELECT
        id,
        call_uuid,
        direction,
        started_at,
        number_digits,
        user_id,
        user_email,
        loaded_at
    FROM {{ ref('stg_aircall__call_created') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        direction,
        started_at,
        number_digits,
        user_id,
        user_email,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_created') }}
)

SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY call_uuid ORDER BY loaded_at DESC) AS __row_number 
    FROM call_created_log)
WHERE __row_number = 1