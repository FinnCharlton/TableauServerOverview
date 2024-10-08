-- Aggregates total views per workbook

select
    workbook_id,
    site,
    SUM(total_views) as total_views
from
    {{ref("stg_views")}}
group by
    workbook_id,
    site