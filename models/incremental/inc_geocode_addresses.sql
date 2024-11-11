WITH address_counts AS (
    SELECT
        TRIM(a.address_pickup) AS address_pickup,
        COUNT(*) AS count
    FROM {{ ref('fct_user_rideshare_trips') }} a
    LEFT JOIN {{ ref('stg_bwk_insights__address_geocoding') }} b 
        ON TRIM(a.address_pickup) = b.address
    WHERE
        a.address_pickup IS NOT NULL AND
        a.address_pickup_zip IS NULL AND
        b.address IS NULL
    GROUP BY TRIM(a.address_pickup)
)

SELECT *
FROM address_counts
ORDER BY count DESC