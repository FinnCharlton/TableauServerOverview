

select
    dsm.workbook_id as workbook_id,
    dsm.datasource_name as datasource_name
from
    {{ref("stg_datasource_mappings")}} dsm
join
    (select
        name
    from
        {{ref("stg_datasources")}}
    group by 1
    ) ds
    
on
    dsm.datasource_name = ds.name