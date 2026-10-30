-- Customer attributes to group metrics by (no personal data: names,
-- email, phone and date of birth are left out).

select
    customer_id,
    gender,
    marital_status,
    customer_city,
    customer_governorate,
    customer_segment,
    occupation
from {{ source('gold', 'dim_customer') }}
