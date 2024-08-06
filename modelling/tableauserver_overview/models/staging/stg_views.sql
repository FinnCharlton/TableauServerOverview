

select 
    "_id" as id,
    "_name" as name,
    "_project_id" as project_id,
    "_workbook_id" as workbook_id,
    "_owner_id" as owner_id,
    "_created_at" as created_at,
    "_updated_at" as last_updated,
    "_content_url" as content_url
from
    {{source('tableauserver_overview','src_views')}}