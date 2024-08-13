{{config(materialized="table")}}

select 
    node,
    x,
    y 
from
    {{ref("int_graph_edges")}}, table(summary_stats(workbook_id, datasource_name) over (partition by 1))
join