with

    source as (select * from {{ source("google_workspace", "users") }}),

    transformation as (

        select
            id google_id,
            primaryemail primary_email,
            fullname full_name,
            givenname first_name,
            familyname last_name,
            isadmin is_admin,
            isdelegatedadmin is_delegated_admin,
            lastlogintime last_login_time,
            creationtime creation_time,
            loadepoch load_epoch
        from source

    )

select *
from transformation
