
select 
    * 
from(

select distinct
    edges.workbook_id,
    edges.datasource_name,
    nodes.site,
    nodes.x,
    nodes.y
from
    {{ref("int_graph_nodes__coords")}} nodes
join
    {{ref("int_graph_edges")}} edges
on
    nodes.node = edges.workbook_id and nodes.site = edges.site

union all

select distinct
    edges.workbook_id,
    edges.datasource_name,
    nodes.site,
    nodes.x,
    nodes.y
from
    {{ref("int_graph_nodes__coords")}} nodes
join
    {{ref("int_graph_edges")}} edges
on
    nodes.node = edges.datasource_name and nodes.site = edges.site

)

order by workbook_id