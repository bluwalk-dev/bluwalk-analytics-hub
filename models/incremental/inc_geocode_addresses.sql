SELECT
    TRIM(a.address_pickup),
    COUNT(*) count
FROM {{ ref('fct_user_rideshare_trips') }} a
LEFT JOIN {{ ref('stg_bwk_insights__address_geocoding') }} b ON a.address_pickup = b.address
WHERE
    a.address_pickup IS NOT NULL AND
    a.address_pickup_zip IS NULL AND
    b.address IS NULL
GROUP BY a.address_pickup
ORDER BY count DESC
LIMIT 100