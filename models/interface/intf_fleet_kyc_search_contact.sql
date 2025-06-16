{% macro clean_pt_vat(col) -%}
  CASE
    WHEN {{ col }} IS NULL THEN NULL
    ELSE
      'PT'
      || REGEXP_REPLACE(
           {{ col }},
           r'(?i)^(PT)|\s+',
           ''
         )
  END
{%- endmacro %}

WITH raw_all AS (

  -------------------------------------------------------------------
  -- 1) HubSpot CONTACTS
  -------------------------------------------------------------------
  SELECT
    CONCAT(a.first_name, ' ', a.last_name)   AS name,                           -- STRING
    CAST(a.property_birthday AS DATE)        AS birthday,                       -- DATE
    CAST(a.property_numero_cartao_cidadao AS STRING) AS citizen_card,           -- STRING
    CAST(a.property_nr_carta_conducao AS STRING)     AS driver_license_nr,      -- STRING
    CAST(NULL AS TIMESTAMP)                  AS manager_from,                 -- TIMESTAMP
    CAST(a.property_city AS STRING)           AS city,                         -- STRING
    CAST(a.property_address AS STRING)        AS street,                       -- STRING
    CAST(NULL AS STRING)                     AS country_id,                   -- STRING
    CAST(a.property_zip AS STRING)            AS zip,                          -- STRING
    CAST(NULL AS STRING)                     AS state_id,                     -- STRING
    CAST(a.property_nif_contacto AS STRING)   AS vat_raw,                      -- STRING
    CAST(a.property_mobilephone AS STRING)    AS phone,                         -- STRING  ← cast here
    CAST(a.email AS STRING)                   AS email,                         -- STRING
    CAST(a.property_mobilephone AS STRING)    AS mobile,                        -- STRING
    'pt_PT'                                  AS lang,                         -- STRING
    'Portugal'                               AS tz,                           -- STRING
    CAST(NULL AS STRING)                     AS role,                          -- STRING
    TRUE                                     AS active,                        -- BOOL
    'contact'                                AS type,                          -- STRING
    FALSE                                    AS is_company,                    -- BOOL
    TRUE                                     AS is_driver,                     -- BOOL
    CAST(c.property_company_vat AS STRING)    AS company_vat_raw               -- STRING

  FROM {{ ref('stg_hubspot_drivfit__contacts') }} AS a
  LEFT JOIN {{ ref('stg_hubspot_drivfit__contact_companies') }} AS b
    ON a.hs_contact_id = b.contact_id
  LEFT JOIN {{ ref('stg_hubspot_drivfit__companies') }} AS c
    ON b.company_id = c.id


  UNION ALL


  -------------------------------------------------------------------
  -- 2) HubSpot COMPANIES
  -------------------------------------------------------------------
  SELECT
    CAST(property_name AS STRING)                 AS name,
    CAST(NULL AS DATE)                           AS birthday,
    CAST(NULL AS STRING)                         AS citizen_card,
    CAST(NULL AS STRING)                         AS driver_license_nr,
    CAST(NULL AS TIMESTAMP)                      AS manager_from,
    CAST(property_city AS STRING)                AS city,
    CAST(property_address AS STRING)             AS street,
    CAST(NULL AS STRING)                         AS country_id,
    CAST(property_zip AS STRING)                 AS zip,
    CAST(NULL AS STRING)                         AS state_id,
    CAST(property_company_vat AS STRING)         AS vat_raw,
    CAST(property_mobile_phone_number AS STRING) AS phone,     -- STRING  ← cast here
    CAST(property_email_platforms AS STRING)     AS email,     -- STRING
    CAST(property_mobile_phone_number AS STRING) AS mobile,    -- STRING
    'pt_PT'                                      AS lang,
    'Portugal'                                   AS tz,
    CAST(NULL AS STRING)                         AS role,
    TRUE                                         AS active,
    'contact'                                    AS type,
    TRUE                                         AS is_company,
    FALSE                                        AS is_driver,
    CAST(property_company_vat AS STRING)         AS company_vat_raw

  FROM {{ ref('stg_hubspot_drivfit__companies') }}

),

cleaned AS (
  SELECT
    *,
    {{ clean_pt_vat('vat_raw') }}         AS vat_clean,
    {{ clean_pt_vat('company_vat_raw') }} AS company_vat_clean
  FROM raw_all
)

SELECT
  c.name,
  c.birthday,
  c.citizen_card,
  c.driver_license_nr,
  c.manager_from,
  c.city,
  c.street,
  c.country_id,
  c.zip,
  c.state_id,
  c.vat_clean       AS individual_vat,
  c.company_vat_clean as company_vat,
  c.phone,
  c.email,
  c.mobile,
  c.lang,
  c.tz,
  c.role,
  c.active,
  c.type,
  c.is_company,
  c.is_driver,
  COALESCE(yco.contact_id, yc.contact_id) AS contact_id

FROM cleaned AS c

  LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} AS yc
    ON yc.contact_vat = c.vat_clean

  LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} AS yco
    ON yco.contact_vat = c.company_vat_clean