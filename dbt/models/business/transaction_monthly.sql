{{ config(materialized='table', indexes=[{'columns': ['month']}]) }}

-- Transactions per month, bank, channel, type and status, for Power BI.
-- Amounts in EGP.

select
    date_trunc('month', fact.transaction_date)::date as month,
    fact.bank_id,
    banks.bank_name,
    fact.channel,
    fact.digital_channel_flag,
    fact.transaction_type,
    fact.transaction_status,
    count(*) as transactions,
    count(distinct fact.customer_id) as customers,
    sum(fact.amount_egp) as amount_egp,
    sum(fact.estimated_channel_cost_egp) as channel_cost_egp,
    count(*) filter (where fact.digital_migration_opportunity) as digital_migration_opportunities
from {{ ref('fct_transactions') }} as fact
left join {{ ref('dim_banks') }} as banks using (bank_id)
group by 1, 2, 3, 4, 5, 6, 7
