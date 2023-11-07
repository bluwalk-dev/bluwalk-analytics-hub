with

source as (
    SELECT *
    FROM {{ source('talkdesk', 'calls') }}
),

transformation as (

    select
        CAST(call_type AS STRING) as call_type,
        IF(
            agent_name = 'If-No-Answer Agent', 'missed', 
            IF(right(call_type,10)='abandoned', 'abandoned', 
                IF( (right(call_type,2)='in' OR right(call_type,3)='out'),'completed'
                ,'missed')
            )
        ) as call_outcome,
        if(left(call_type,2) = 'in', 'inbound', 'outbound') call_direction,
        CAST(interaction_id AS STRING) as interaction_id,
        CAST(ring_group AS STRING) as ring_group,
        CAST(dedicated_line AS BOOLEAN) as dedicated_line,
        CAST(call_start_time AS DATETIME) as call_start_time,
        CAST(call_end_time AS DATETIME) as call_end_time,
        FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(call_start_time AS STRING), 'Europe/Lisbon')) AS call_start_timestamp,
        FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(call_end_time AS STRING), 'Europe/Lisbon')) AS call_end_timestamp,
        IF(bluwalk_phone = 'N/A', '',LEFT(bluwalk_phone, 13)) as internal_line_nr,
        CAST(customer_phone AS STRING) customer_phone_nr,
        CAST(IF(agent_name = 'If-No-Answer Agent', NULL, agent_name) AS STRING) as agent_name,
        CAST(talk_time AS TIME) as duration,
        EXTRACT(second from talk_time) duration_in_seconds,
        CAST(disposition AS STRING) as disposition,
        CAST(rating AS STRING) as rating,
        CAST(recording_link AS STRING) as recording_link
    from source

)

SELECT * FROM transformation
WHERE interaction_id IS NOT NULL