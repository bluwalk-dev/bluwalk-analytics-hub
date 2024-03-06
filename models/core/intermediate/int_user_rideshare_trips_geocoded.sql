WITH trips AS (
  SELECT
    a.request_local_time,
    a.address_pickup,
    CASE 
      WHEN c.latitude IS NOT NULL THEN c.latitude
      WHEN b.latitude IS NOT NULL THEN b.latitude
      ELSE NULL
    END latitude,
    CASE 
      WHEN c.longitude IS NOT NULL THEN c.longitude
      WHEN b.longitude IS NOT NULL THEN b.longitude
      ELSE NULL
    END longitude
  FROM {{ ref('fct_user_rideshare_trips') }} a
  LEFT JOIN (
        SELECT * 
        FROM {{ ref('stg_bwk_insights__address_geocoding') }} 
        WHERE
            geo_accuracy = 'good' AND
            zip_accuracy = 'good'
    ) b ON a.address_pickup = b.address
  LEFT JOIN {{ ref('stg_bwk_insights__zip_codes') }} c ON a.address_pickup_zip = c.zip_code
)

SELECT DISTINCT
    TO_HEX(MD5(t.request_local_time || t.latitude || t.longitude)) as trip_key,
    t.*,
    z.location_id,
    z.location_name
FROM trips t
CROSS JOIN {{ ref('dim_locations') }} z
WHERE
  ST_CONTAINS(z.location_polygon, ST_GEOGPOINT(t.longitude, t.latitude)) AND 
  latitude IS NOT NULL AND 
  longitude IS NOT NULL
ORDER BY request_local_time DESC