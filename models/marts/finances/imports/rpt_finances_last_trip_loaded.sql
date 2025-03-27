SELECT
    partner_name,
    partner_login_location,
    max(request_local_time)
FROM {{ ref('fct_user_rideshare_trips') }}
WHERE 
    partner_name IS NOT NULL AND
    partner_login_location IS NOT NULL
GROUP BY 
    partner_name,
    partner_login_location