-- Headline measures computed in SQL from gold, to check the Power BI
-- measures (semantic/fact_transaction_measures.dax, EVALUATE block):
-- with no filters, each number must match its measure.
--
--   psql -U metaflow -d metaflow -f semantic/measure_checks.sql

WITH f AS (SELECT * FROM gold.fact_transaction),
digital_cost AS (
    SELECT sum(estimated_channel_cost_egp) / count(*) AS per_txn
    FROM f WHERE digital_channel_flag
),
opp AS (
    SELECT count(*) AS n, sum(estimated_channel_cost_egp) AS cost
    FROM f WHERE digital_migration_opportunity
)
SELECT
    count(*)                                                        AS "Transactions",
    round(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed'), 2)
                                                                    AS "Transaction Value (EGP)",
    round(sum(transaction_fee * amount_egp / NULLIF(transaction_amount, 0))
        FILTER (WHERE transaction_status = 'Completed'), 2)         AS "Fee Revenue (EGP)",
    round(sum(estimated_channel_cost_egp), 2)                       AS "Channel Cost (EGP)",
    round(avg(digital_channel_flag::int), 4)                        AS "Digital Share %",
    round(count(DISTINCT customer_id) FILTER (WHERE digital_channel_flag)::numeric
          / count(DISTINCT customer_id), 4)                         AS "Digital Adoption Rate %",
    (SELECT n FROM opp)                                             AS "Migration Opportunities",
    round(greatest(0, (SELECT cost FROM opp)
                - (SELECT n FROM opp) * (SELECT per_txn FROM digital_cost)), 2)
                                                                    AS "Potential Cost Savings (EGP)",
    round(avg((transaction_status = 'Failed')::int), 4)             AS "Failure Rate %",
    count(DISTINCT customer_id)                                     AS "Active Customers"
FROM f;

-- Digital share by channel and the cost gap behind the migration case.
SELECT channel,
       digital_channel_flag,
       count(*)                                          AS transactions,
       round(sum(estimated_channel_cost_egp) / count(*), 2) AS cost_per_transaction,
       count(*) FILTER (WHERE digital_migration_opportunity) AS migration_opportunities
FROM gold.fact_transaction
GROUP BY 1, 2
ORDER BY transactions DESC;
