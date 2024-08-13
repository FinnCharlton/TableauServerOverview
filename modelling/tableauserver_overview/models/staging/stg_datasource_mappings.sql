

select
    "workbook_id" as workbook_id,
    "datasource_name" as datasource_name,
    "site" as site
from
    {{source('tableauserver_overview', 'src_datasource_mappings')}}