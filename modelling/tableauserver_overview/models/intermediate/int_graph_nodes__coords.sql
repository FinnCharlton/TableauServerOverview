{{config(materialized="table")}}

select 
    node,
    x,
    y,
    ts_site as site
from
    {{ref("int_graph_edges")}}, table(network_graph(workbook_id, datasource_name, site) over (partition by site))