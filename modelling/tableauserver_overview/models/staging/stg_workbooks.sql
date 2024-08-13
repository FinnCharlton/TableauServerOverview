
select 
    "_id" as id,
    "name" as name,
    "_project_id" as project_id,
    "_project_name" as project_name,
    "owner_id" as owner_id,
    "site" as site,
    "_created_at" as created_at,
    "_updated_at" as last_updated,
    "_description" as description,
    "_webpage_url" as url
from 
    {{source('tableauserver_overview','src_workbooks')}}
    