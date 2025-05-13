with

source as (
    select
        *
    from {{ source('segment', 'nps_response_submitted') }} 
),

transformation as (

    SELECT

        CAST(id AS STRING) event_id,
        CAST(user_id AS INT64) user_id,
        cast(nps_score as int64) nps_response,
        CAST(score_reason AS STRING) nps_response_comments,
        CAST(original_timestamp AS TIMESTAMP) original_timestamp,
        CAST(loaded_at AS TIMESTAMP) loaded_at

    FROM source

)

SELECT * FROM transformation