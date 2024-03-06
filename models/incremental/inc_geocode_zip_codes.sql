SELECT
  a.address_pickup_zip,
  COUNT(*) count
FROM {{ ref('fct_user_rideshare_trips') }} a
LEFT JOIN {{ ref('stg_bwk_insights__zip_codes') }} b ON a.address_pickup_zip = b.zip_code
WHERE
    a.address_pickup_zip IS NOT NULL AND
    b.latitude IS NULL AND
    b.longitude IS NULL
GROUP BY a.address_pickup_zip
ORDER BY count DESC
LIMIT 100