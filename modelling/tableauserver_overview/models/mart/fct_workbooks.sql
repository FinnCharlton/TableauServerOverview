

select
    wbooks.*,
    users.full_name as owner,
    usage.total_views
from
    {{ref("stg_workbooks")}} wbooks
left join
    {{ref("stg_users")}} users
on
    wbooks.owner_id = users.id and wbooks.site = users.site
left join
    {{ref("int_usage_per_workbook")}} usage
on
    wbooks.id = usage.workbook_id and wbooks.site = usage.site