

select
    nodes.id as id,
    nodes.name as name,
    nodes.asset_type as asset_type,
    nodes.site,
    case
        when ds.owners is null
        then wb.owner
        else ds.owners
        end as owner,
    case
        when ds.created_at is null
        then wb.created_at
        else ds.created_at
        end as created_at,
    coords.x as x,
    coords.y as y
from
    {{ref("int_graph_nodes")}} nodes
join
    {{ref("int_graph_nodes__coords")}} coords
on
    nodes.id = coords.node and nodes.site = coords.site
left join
    (
    select
        name,
        site,
        listagg(owner,'|') as owners,
        min(created_at) as created_at
    from
        {{ref("fct_datasources")}}
    group by
        name,
        site
        ) ds
on 
    nodes.id = ds.name and nodes.site = ds.site
left join
    {{ref("fct_workbooks")}} wb
on
    nodes.id = wb.id and nodes.site = wb.site