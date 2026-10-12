{{ config(materialized='view') }}

select branch_id, branch_name, branch_city, branch_governorate, branch_region
from {{ source('gold', 'dim_branch') }}
