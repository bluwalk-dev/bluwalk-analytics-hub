WITH trip_points AS (
  SELECT
    trip_key, -- Assuming there's an identifier for each trip
    request_local_time,
    address_pickup,
    location_id,
    location_name,
    ST_GEOGPOINT(longitude, latitude) as trip_point
  FROM {{ ref('int_user_rideshare_trips_geocoded') }}
),
zone_points AS (
  SELECT
    zone_name,
    ST_GEOGPOINT(longitude, latitude) as zone_point
  FROM {{ ref('stg_bwk_insights__zones') }}
  WHERE status = 'active'
),
distances AS (
  SELECT
    t.trip_key,
    t.request_local_time,
    t.address_pickup,
    t.location_id,
    t.location_name,
    z.zone_name,
    ST_DISTANCE(t.trip_point, z.zone_point) as distance
  FROM trip_points t
  CROSS JOIN zone_points z
),
filtered_distances AS (
  SELECT
    trip_key,
    request_local_time,
    address_pickup,
    location_id,
    location_name,
    zone_name,
    distance,
    ROW_NUMBER() OVER(PARTITION BY trip_key ORDER BY distance ASC) as rn
  FROM distances
  WHERE distance <= 2000
)
SELECT
    trip_key,
    request_local_time,
    address_pickup,
    location_id,
    location_name,
    zone_name
FROM filtered_distances
WHERE rn = 1