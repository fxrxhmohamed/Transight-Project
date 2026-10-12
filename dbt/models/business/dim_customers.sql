{{ config(materialized='view') }}

-- Descriptive attributes only (no names, contact details or birth dates).

select customer_id, gender, marital_status, customer_city, customer_governorate,
       customer_segment, occupation
from {{ source('gold', 'dim_customer') }}
