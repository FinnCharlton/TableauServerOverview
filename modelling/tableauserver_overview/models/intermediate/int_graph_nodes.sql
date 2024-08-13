

select
    id,
    name,
    'workbook' as asset_type
from
    {{ref("stg_workbooks")}}

union all

select
    name as id,
    name,
    'datasource' as asset_type
from
    {{ref("stg_datasources")}}