
select 
    * 
from(

select distinct
    edges.workbook_id,
    edges.datasource_name,
    nodes.x,
    nodes.y
from
    {{ref("int_graph_nodes__coords")}} nodes
join
    {{ref("int_graph_edges")}} edges
on
    nodes.node = edges.workbook_id

union all

select distinct
    edges.workbook_id,
    edges.datasource_name,
    nodes.x,
    nodes.y
from
    {{ref("int_graph_nodes__coords")}} nodes
join
    {{ref("int_graph_edges")}} edges
on
    nodes.node = edges.datasource_name

)

order by workbook_id