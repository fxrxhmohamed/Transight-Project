-- The gold fact table as MetricFlow reads it: one row per transaction,
-- money in EGP. A view: gold.py may replace the gold tables (dropping this
-- view with them); "python dq.py semantic" builds it again.
--
-- transaction_fee is in the transaction's currency: fee_egp converts it
-- with the row's own rate (amount_egp / transaction_amount).

select
    transaction_id,
    transaction_date,
    transaction_type,
    transaction_status,
    channel,
    channel_feature,
    currency,
    failure_reason,
    coalesce(digital_channel_flag, false)          as digital_channel_flag,
    coalesce(digital_migration_opportunity, false) as digital_migration_opportunity,
    transaction_status = 'Completed'               as is_completed,
    coalesce(transaction_fee > 0, false)           as has_fee,
    card_type,
    card_type is not null                          as is_card,
    merchant_name,
    login_success,
    coalesce(attempt_count > 1, false)             as is_retried,
    coalesce(attempt_count = 1, false)             as is_first_attempt,
    amount_egp,
    transaction_fee * amount_egp / nullif(transaction_amount, 0) as fee_egp,
    estimated_channel_cost_egp,
    attempt_count,
    session_duration_seconds,
    account_id,
    customer_id,
    branch_id,
    bank_id,
    run_id
from {{ source('gold', 'fact_transaction') }}
