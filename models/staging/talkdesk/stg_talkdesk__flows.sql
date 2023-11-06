with

    source as (
        select * 
        from {{ source("talkdesk", "flows") }}
    ),

    transformation as (

        select
            cast(call_sid as string) call_sid,
            cast(interaction_id as string) interaction_id,
            cast(destination_number as string) destination_number,
            cast(origin_number as string) origin_number,
            cast(flow_name as string) flow_name,
            cast(flow_id as string) flow_id,
            cast(component_title as string) component_title,
            cast(step_name as string) step_name,
            cast(exit as string) exit,
            cast(sec_in_step as int64) sec_in_step,
            cast(timestamp as timestamp) timestamp,
            cast(step_execution_order as int64) step_execution_order
        from source

    )

select *
from transformation
where interaction_id is not null
