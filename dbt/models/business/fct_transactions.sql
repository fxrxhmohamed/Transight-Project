{{ config(materialized='view') }}

-- The gold fact table as MetricFlow sees it. A view: gold.py replaces the
-- gold tables (dropping this view with them), and "python dq.py gold"
-- builds it again right after.

select
    transaction_id,
    transaction_date,
    transaction_type,
    transaction_status,
    channel,
    digital_channel_flag,
    digital_migration_opportunity,
    currency,
    amount_egp,
    estimated_channel_cost_egp,
    account_id,
    customer_id,
    branch_id,
    bank_id
from {{ source('gold', 'fact_transaction') }}
