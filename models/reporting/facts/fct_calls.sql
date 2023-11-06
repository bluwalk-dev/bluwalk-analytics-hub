/*WITH talkdesk AS (
    SELECT 
        interactionId uuid,
        'talkdesk' supplier,
        if(left(callType,2) = 'in', 'inbound', 'outbound') callDirection,
        IF(right(callType,10)='abandoned',
            'abandoned', IF( (right(callType,2)='in' OR right(callType,3)='out'),'completed','missed')
        ) as callOutcome,
        callStartTime,
        callEndTime, 
        left(bluwalkPhone, 13) bluwalkPhone,
        customerPhone, 
        EXTRACT(second from talkTime) talkTime,
        agentName,
        '' teamName,
        recordingLink 
    FROM {{ ref('stg_talkdesk_calls') }}
), aircall AS (
    SELECT
        cc.call_uuid uuid,
        'aircall' supplier,
        cc.direction callDirection,
        IF(cc.direction = 'Inbound', 
            IF(ca.id IS NOT NULL, "completed",
            IF(cra.id IS NOT NULL, "missed", "abandoned")
            ), IF(ca.id IS NOT NULL, "completed",'missed'
            )
        ) as callOutcome,
        DATETIME(TIMESTAMP_SECONDS(cc.started_at), 'Europe/Lisbon') callStartTime, 
        DATETIME(TIMESTAMP_SECONDS(ce.ended_at), 'Europe/Lisbon') callEndTime, 
        REPLACE(cc.number_digits, ' ','') bluwalkPhone, 
        cc.user_id customerPhone,
        ce.duration talkTime, 
        if(cc.user_email IS NULL, ca.user_email, cc.user_email) agentName, 
        ce.number_name teamName,
        ce.asset recordingLink
    FROM {{ ref('stg_aircall_call_created') }} cc
    LEFT JOIN {{ ref('stg_aircall_call_answered') }} ca ON cc.call_uuid = ca.call_uuid
    LEFT JOIN {{ ref('stg_aircall_call_ended') }} ce ON cc.call_uuid = ce.call_uuid
    LEFT JOIN {{ ref('stg_aircall_call_ringing_on_agent') }} cra ON cc.call_uuid = cra.call_uuid
), aircallV2 AS (
    SELECT
        cc.call_uuid uuid,
        'aircall' supplier,
        cc.direction callDirection,
        IF(cc.direction = 'Inbound', 
            IF(ca.id IS NOT NULL, "completed",
            IF(cra.id IS NOT NULL, "missed", "abandoned")
            ), IF(ca.id IS NOT NULL, "completed", 'missed'
            )
        ) as callOutcome,
        DATETIME(TIMESTAMP_SECONDS(cc.started_at), 'Europe/Lisbon') callStartTime, 
        DATETIME(TIMESTAMP_SECONDS(ce.ended_at), 'Europe/Lisbon') callEndTime, 
        REPLACE(cc.number_digits, ' ','') bluwalkPhone, 
        cc.user_id customerPhone,
        ce.duration talkTime, 
        if(cc.user_email IS NULL, ca.user_email, cc.user_email) agentName,
        ce.number_name teamName,
        ce.asset recordingLink
    FROM {{ ref('stg_aircallV2_call_created') }} cc
    LEFT JOIN {{ ref('stg_aircallV2_call_answered') }} ca ON cc.call_uuid = ca.call_uuid
    LEFT JOIN {{ ref('stg_aircallV2_call_ended') }} ce ON cc.call_uuid = ce.call_uuid
    LEFT JOIN {{ ref('stg_aircallV2_call_ringing_on_agent') }} cra ON cc.call_uuid = cra.call_uuid
)

SELECT * FROM (
    SELECT * FROM talkdesk
    UNION ALL 
    SELECT * FROM aircall
    UNION ALL
    SELECT * FROM aircallV2)
ORDER BY callEndTime DESC*/