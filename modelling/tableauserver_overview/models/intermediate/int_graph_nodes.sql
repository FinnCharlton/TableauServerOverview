

select
    id,
    name,
    site,
    'workbook' as asset_type
from
    {{ref("stg_workbooks")}}

union all

select
    name as id,
    name,
    site,
    'datasource' as asset_type
from
    {{ref("stg_datasources")}}