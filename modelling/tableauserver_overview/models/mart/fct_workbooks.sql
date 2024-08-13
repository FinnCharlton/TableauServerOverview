

select
    wbooks.*,
    usage.total_views
from
    {{ref("stg_workbooks")}} wbooks
left join
    {{ref("int_usage_per_workbook")}} usage
on
    wbooks.id = usage.workbook_id