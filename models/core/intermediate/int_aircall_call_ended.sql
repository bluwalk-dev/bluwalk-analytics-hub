WITH call_ended_log AS (
    SELECT
        _id as db_id,
        id,
        call_uuid,
        ended_at,
        duration,
        number_name,
        missed_call_reason,
        asset,
        loaded_at
    FROM {{ ref('stg_aircall__call_ended') }}
    UNION ALL
    SELECT
        _id as db_id,
        id,
        call_uuid,
        ended_at,
        duration,
        number_name,
        missed_call_reason,
        asset,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_ended') }}
), 
merged_calls AS (
    SELECT * EXCEPT (__row_number) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY call_uuid ORDER BY loaded_at DESC) AS __row_number 
        FROM call_ended_log)
    WHERE __row_number = 1
)

SELECT
    db_id,
    id,
    call_uuid,
    TIMESTAMP_SECONDS(ended_at) ended_at,
    duration,
    number_name,
    missed_call_reason,
    asset recording_url
FROM merged_calls