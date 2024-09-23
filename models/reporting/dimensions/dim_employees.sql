WITH googleusers AS (
    SELECT *
    FROM (
        SELECT 
            a.*,
            ROW_NUMBER() OVER (PARTITION BY primary_email ORDER BY load_epoch DESC) AS row_num
        FROM {{ ref("stg_google_workspace__users") }} a
    ) ranked_googleusers
    WHERE row_num = 1
),
-- Select the most recent record for each employee_id from stg_odoo__hr_employees
ranked_employees AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY write_date DESC) AS row_num
    FROM {{ ref("stg_odoo__hr_employees") }}
)
SELECT DISTINCT
    a.id AS employee_id,
    d.first_name AS employee_first_name,
    d.last_name AS employee_last_name,
    CONCAT(d.first_name, ' ', d.last_name) AS employee_short_name,
    a.name AS employee_full_name,
    a.user_partner_id AS employee_contact_id,
    a.user_id AS employee_user_id,
    a.active AS employee_active,
    a.work_email AS employee_email,
    aa.name AS employee_team,
    a.admission_date AS employee_admission_date,
    a.resignation_date AS employee_resignation_date,
    b.country_name AS employee_country_of_birth,
    a.work_location AS employee_work_location,
    a.gender AS employee_gender,
    a.birthday AS employee_birthdate,
    a.primavera_code AS employee_primavera_id,
    d.google_id AS employee_google_id,
    e.hubspot_user_id AS employee_hubspot_user_id,
    e.hubspot_owner_id AS employee_hubspot_owner_id,
    e.hubspot_team_name AS employee_hubspot_team,
    d.last_login_time AS employee_last_google_login,
    a.write_date AS updated_at
FROM ranked_employees a
LEFT JOIN {{ ref("stg_odoo__hr_departments") }} aa ON a.department_id = aa.id
LEFT JOIN {{ ref("dim_countries") }} b ON a.country_id = b.country_id
LEFT JOIN googleusers d ON a.work_email = d.primary_email
LEFT JOIN {{ ref("base_hubspot_users") }} e ON a.work_email = e.email
WHERE a.row_num = 1  -- Only select the latest record for each employee
ORDER BY a.id DESC