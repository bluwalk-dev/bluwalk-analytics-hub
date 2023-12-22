SELECT 
    date, 
    'Facebook' source,
    page_reach_organic organic_reach,
    page_reach total_reach
FROM {{ ref('stg_facebook__reach') }}

UNION ALL

SELECT 
    date,
    'Instagram' source,
    NULL organic_reach,
    reach total_reach
FROM {{ ref('stg_instagram__user_insights') }}

ORDER BY date DESC, source