SELECT
    *,
    CASE
        WHEN CAST(created_at AS DATE) >= '2024-06-01' THEN 'Service'
        ELSE (
            CASE 
                WHEN number_name IN ('Main Number', 'Customer Service') THEN 'Service'
                ELSE 'Activation'
            END
        )
    END team
FROM {{ ref('base_calls') }}
WHERE missed_call_reason = 'no_available_agent'