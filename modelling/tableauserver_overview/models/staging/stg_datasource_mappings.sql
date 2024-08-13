

select
    "workbook_id" as workbook_id,
    "datasource_name" as datasource_name
from
    {{source('tableauserver_overview', 'src_datasource_mappings')}}