SELECT
    date, 
    'Facebook' source, 
    page_content_activity total_activity,
    page_fans total_followers
FROM {{ ref('stg_facebook__impressions') }}
ORDER BY date DESC, source