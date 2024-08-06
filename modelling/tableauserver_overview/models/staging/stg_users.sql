

select 
    "_id" as id,
    "_name" as name,
    "fullname" as full_name,
    "_site_role" as role,
    "_last_login" as last_login
from
    {{source('tableauserver_overview','src_users')}}