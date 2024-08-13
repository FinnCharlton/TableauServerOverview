

select
    nodes.id as id,
    nodes.name as name,
    nodes.asset_type as asset_type,
    coords.x as x,
    coords.y as y
from
    {{ref("int_graph_nodes")}} nodes
join
    {{ref("int_graph_nodes__coords")}} coords
on
    nodes.id = coords.node