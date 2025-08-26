with

source as (
    SELECT *
    FROM {{ source('zendesk', 'src_zendesk_tickets') }}
),

transformation as (

    select
        assignee_id,
        brand_id,
        car_accident_category,
        created_at,
        description,
        group_id,
        id,
        loaded_at,
        received_at,
        recipient,
        requester_id,
        status,
        subject,
        submitter_id,
        tags,
        theme,
        ticket_form_id,
        updated_at,
        url,
        uuid_ts,
        collaborator_ids,
        external_id,
        priority,
        satisfaction_rating_score,
        topic,
        type,
        satisfaction_rating_id,
        satisfaction_rating_reason,
        satisfaction_rating_reason_id,
        satisfaction_rating_comment,
        aircall_call_id,
        duration,
        talk_time,
        waiting_time
    from source

)

SELECT * FROM transformation