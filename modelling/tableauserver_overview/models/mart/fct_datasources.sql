

select
    ds.*,
    us.full_name as owner
from
    {{ref("stg_datasources")}} ds
left join
    {{ref("stg_users")}} us
on
    ds.owner_id = us.id and ds.site = us.site