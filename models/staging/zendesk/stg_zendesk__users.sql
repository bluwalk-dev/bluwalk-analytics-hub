with

source as (
    SELECT *
    FROM {{ source('zendesk', 'users') }}
),

transformation as (

    select
        CAST(active AS BOOL) AS active,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(custom_role_id AS INT64) AS custom_role_id,
        CAST(email AS STRING) AS email,
        CAST(external_id AS STRING) AS external_id,
        CAST(id AS STRING) AS id,
        CAST(last_login_at AS TIMESTAMP) AS last_login_at,
        CAST(loaded_at AS TIMESTAMP) AS loaded_at,
        CAST(locale AS STRING) AS locale,
        CAST(locale_id AS INT64) AS locale_id,
        CAST(moderator AS BOOL) AS moderator,
        CAST(name AS STRING) AS name,
        CAST(notes AS STRING) AS notes,
        CAST(only_private_comments AS BOOL) AS only_private_comments,
        CAST(organization_id AS INT64) AS organization_id,
        CAST(phone AS STRING) AS phone,
        CAST(received_at AS TIMESTAMP) AS received_at,
        CAST(restricted_agent AS BOOL) AS restricted_agent,
        CAST(role AS STRING) AS role,
        CAST(shared AS BOOL) AS shared,
        CAST(shared_agent AS BOOL) AS shared_agent,
        CAST(suspended AS BOOL) AS suspended,
        CAST(ticket_restriction AS STRING) AS ticket_restriction,
        CAST(time_zone AS STRING) AS time_zone,
        CAST(two_factor_auth_enabled AS BOOL) AS two_factor_auth_enabled,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(url AS INT64) AS url,
        CAST(user_fields_account_service_city AS STRING) AS user_fields_account_service_city,
        CAST(user_fields_account_type AS STRING) AS user_fields_account_type,
        CAST(user_fields_active_booking AS FLOAT64) AS user_fields_active_booking,
        CAST(user_fields_active_rentals AS BOOL) AS user_fields_active_rentals,
        CAST(user_fields_balance AS FLOAT64) AS user_fields_balance,
        CAST(user_fields_birthday AS TIMESTAMP) AS user_fields_birthday,
        CAST(user_fields_id AS STRING) AS user_fields_id,
        CAST(user_fields_language AS STRING) AS user_fields_language,
        CAST(user_fields_total_earnings AS FLOAT64) AS user_fields_total_earnings,
        CAST(user_fields_total_partners_activated AS FLOAT64) AS user_fields_total_partners_activated,
        CAST(user_fields_total_rentals AS FLOAT64) AS user_fields_total_rentals,
        CAST(user_fields_total_trips_courier AS FLOAT64) AS user_fields_total_trips_courier,
        CAST(user_fields_total_trips_food_delivery AS FLOAT64) AS user_fields_total_trips_food_delivery,
        CAST(user_fields_total_trips_made AS FLOAT64) AS user_fields_total_trips_made,
        CAST(user_fields_total_trips_rideshare AS FLOAT64) AS user_fields_total_trips_rideshare,
        CAST(user_fields_total_trips_shopping AS FLOAT64) AS user_fields_total_trips_shopping,
        CAST(user_fields_tvde_license AS BOOL) AS user_fields_tvde_license,
        CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
        CAST(verified AS BOOL) AS verified,
        CAST(user_fields_phone AS BOOL) AS user_fields_phone,
        CAST(user_fields_promo_code AS STRING) AS user_fields_promo_code,
        CAST(user_fields_referral_code AS STRING) AS user_fields_referral_code,
        CAST(user_fields_address_city AS STRING) AS user_fields_address_city,
        CAST(user_fields_address_country AS STRING) AS user_fields_address_country,
        CAST(user_fields_address_postal_code AS STRING) AS user_fields_address_postal_code,
        CAST(user_fields_address_state AS STRING) AS user_fields_address_state,
        CAST(user_fields_address_street AS STRING) AS user_fields_address_street,
        CAST(user_fields_tax_id AS STRING) AS user_fields_tax_id
    from source

)

SELECT * FROM transformation