SELECT
*
FROM {{ ref("base_evio_charging_sessions") }}
WHERE energy_id is null