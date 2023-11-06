{{ config(alias="dim_employees") }}

with
    googleusers as (
        select a.*
        from {{ ref("stg_google_workspace__users") }} a
        left join
            (
                select google_id, max(load_epoch) load_epoch
                from {{ ref("stg_google_workspace__users") }}
                group by google_id
            ) b
            on a.google_id = b.google_id
            and a.load_epoch = b.load_epoch
        where b.google_id is not null

    )

SELECT

    a.id employee_id,
    d.first_name employee_first_name,
    d.last_name employee_last_name,
    concat(d.first_name, ' ', d.last_name) employee_short_name,
    a.name employee_full_name,
    a.user_partner_id employee_contact_id,
    a.user_id employee_user_id,
    a.active employee_active,
    a.work_email employee_email,
    aa.name employee_team,
    a.admission_date employee_admission_date,
    a.resignation_date employee_resignation_date,
    b.country_name employee_country_of_birth,
    a.work_location employee_work_location,
    a.gender employee_gender,
    a.birthday employee_birthdate,
    a.primavera_code employee_primavera_id,
    d.google_id employee_google_id,
    e.hubspot_user_id employee_hubspot_user_id,
    e.hubspot_owner_id employee_hubspot_owner_id,
    e.hubspot_team_name employee_hubspot_team,
    d.last_login_time employee_last_google_login

FROM {{ ref("stg_odoo__hr_employees") }} a
LEFT JOIN {{ ref("stg_odoo__hr_departments") }} aa ON a.department_id = aa.id
LEFT JOIN {{ ref("dim_countries") }} b ON a.country_id = b.country_id
LEFT JOIN {{ ref("dim_contacts") }} c ON a.user_partner_id = c.contact_id
LEFT JOIN googleusers d ON a.work_email = d.primary_email
LEFT JOIN {{ ref("base_hubspot_users") }} e ON a.work_email = e.email
ORDER BY a.id DESC