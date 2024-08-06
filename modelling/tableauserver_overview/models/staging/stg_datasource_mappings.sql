

select
    "workbook_id" as workbook_id,
    "datasource_ids" as datasource_id
from
    {{source('tableauserver_overview', 'src_datasource_mappings')}}