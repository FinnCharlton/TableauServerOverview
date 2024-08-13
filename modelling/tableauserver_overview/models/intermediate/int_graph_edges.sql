

select
    dsm.workbook_id as workbook_id,
    dsm.datasource_name as datasource_name,
    dsm.site as site
from
    {{ref("stg_datasource_mappings")}} dsm
join
    (select
        name,
        site
    from
        {{ref("stg_datasources")}}
    group by 
        name, 
        site
    ) ds
    
on
    dsm.datasource_name = ds.name and dsm.site = ds.site