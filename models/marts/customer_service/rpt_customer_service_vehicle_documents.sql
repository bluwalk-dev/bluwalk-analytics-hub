{{ config(materialized='table') }}

WITH active_vehicles AS (
    SELECT * 
    FROM {{ ref("dim_vehicle_contracts") }}
    WHERE end_date IS NULL AND vehicle_contract_state = 'open'
),

bolt_last_request as (
    SELECT DISTINCT
        a.vehicle_plate,
        b.partner_login_id
    FROM (
        select vehicle_plate, max(request_timestamp) last_request
        from {{ ref('base_bolt_trips') }}
        group by vehicle_plate
    ) a
    LEFT JOIN {{ ref('base_bolt_trips') }} b on a.vehicle_plate = b.vehicle_plate and b.request_timestamp = a.last_request
    WHERE partner_login_id IS NOT NULL
),

uber_last_request as (
    SELECT DISTINCT
        a.vehicle_plate,
        b.partner_login_id
    FROM (
        select vehicle_plate, max(request_timestamp) last_request
        from {{ ref('base_uber_trips') }}
        group by vehicle_plate
    ) a
    LEFT JOIN {{ ref('base_uber_trips') }} b on a.vehicle_plate = b.vehicle_plate and b.request_timestamp = a.last_request
    WHERE partner_login_id IS NOT NULL
)

SELECT * FROM
    (SELECT
        c.user_vat,
        c.user_name,
        'Bolt' partner,
        b.vehicle_contract_type,
        b.vehicle_plate,
        d.org_name account_login_name,
        a.document_name,
        CASE 
            WHEN a.expiration_date < current_date THEN 'expired'
            ELSE 'active'
        END document_status,
        a.expiration_date
    FROM {{ ref("base_bolt_vehicle_documents") }} a
    LEFT JOIN active_vehicles b ON a.vehicle_plate = b.vehicle_plate
    LEFT JOIN {{ ref("dim_users") }} c ON b.contact_id = c.contact_id
    LEFT JOIN {{ ref("dim_partners_logins") }} d ON CAST(a.company_id AS STRING) = d.login_id
    LEFT JOIN bolt_last_request e ON e.vehicle_plate = b.vehicle_plate
    WHERE 
        b.vehicle_plate IS NOT NULL AND
        d.partner_key = 'e8d8ba0b559bd193d89e320f44686eee' AND
        e.partner_login_id = a.company_id

    UNION ALL

    SELECT
        c.user_vat,
        c.user_name,
        'Uber' partner,
        b.vehicle_contract_type,
        b.vehicle_plate,
        d.org_name account_login_name,
        a.document_type_name document_name,
        a.document_status,
        CAST(a.document_expires_at AS DATE) expiration_date
    FROM {{ ref("base_uber_vehicle_documents") }} a
    LEFT JOIN active_vehicles b ON a.vehicle_plate = b.vehicle_plate
    LEFT JOIN {{ ref("dim_users") }} c ON b.contact_id = c.contact_id
    LEFT JOIN {{ ref("dim_partners_logins") }} d ON CAST(a.vehicle_owner_id AS STRING) = d.login_id
    LEFT JOIN uber_last_request e ON e.vehicle_plate = b.vehicle_plate
    WHERE
        b.vehicle_plate IS NOT NULL AND
        d.partner_key = 'a6bedefed0135d2ca14b8a1eb7512a50' AND
        e.partner_login_id = CAST(a.vehicle_owner_id AS STRING)
)
WHERE (expiration_date < DATE_ADD(current_date, INTERVAL 60 DAY) OR expiration_date IS NULL)
ORDER BY expiration_date ASC