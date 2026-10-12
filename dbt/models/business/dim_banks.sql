{{ config(materialized='view') }}

select bank_id, bank_name, bank_type
from {{ source('gold', 'dim_bank') }}
