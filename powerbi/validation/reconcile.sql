-- Figures the Transight Banking report shows, computed in PostgreSQL, to
-- check Power BI against the database (powerbi/validation/reconcile.dax
-- returns the same columns from the model). Headline business numbers are
-- also in semantic/measure_checks.sql.
--
--   psql -U metaflow -d metaflow -f powerbi/validation/reconcile.sql

-- 1. Executive overview, no filters
WITH f AS (SELECT * FROM gold.fact_transaction),
digital_cost AS (
    SELECT sum(estimated_channel_cost_egp) / count(*) AS per_txn FROM f WHERE digital_channel_flag
),
opp AS (
    SELECT count(*) AS n, sum(estimated_channel_cost_egp) AS cost FROM f WHERE digital_migration_opportunity
)
SELECT
    count(*)                                                                     AS transactions,
    round(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed'), 2)    AS transaction_value_egp,
    round(sum(transaction_fee * amount_egp / NULLIF(transaction_amount, 0))
          FILTER (WHERE transaction_status = 'Completed'), 2)                    AS fee_revenue_egp,
    round(sum(estimated_channel_cost_egp), 2)                                    AS channel_cost_egp,
    round(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed')
          / count(*) FILTER (WHERE transaction_status = 'Completed'), 2)         AS avg_transaction_value_egp,
    round(avg((transaction_status = 'Failed')::int), 6)                          AS failure_rate,
    round(avg(digital_channel_flag::int), 6)                                     AS digital_share,
    round(count(DISTINCT customer_id) FILTER (WHERE digital_channel_flag)::numeric
          / count(DISTINCT customer_id), 6)                                      AS digital_adoption_rate,
    count(*) FILTER (WHERE NOT digital_channel_flag)                             AS assisted_transactions,
    (SELECT n FROM opp)                                                          AS migration_opportunities,
    round(greatest(0, (SELECT cost FROM opp) - (SELECT n FROM opp) * (SELECT per_txn FROM digital_cost)), 2)
                                                                                 AS potential_cost_savings_egp,
    round(sum(transaction_fee * amount_egp / NULLIF(transaction_amount, 0))
          FILTER (WHERE transaction_status = 'Completed')
          / sum(estimated_channel_cost_egp), 6)                                  AS fee_coverage
FROM f;

-- 2. By year: volume, completed value and their growth
SELECT extract(year FROM transaction_date)::int                                  AS year,
       count(*)                                                                  AS transactions,
       round(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed'), 2) AS transaction_value_egp,
       round(count(*)::numeric / lag(count(*)) OVER (ORDER BY 1) - 1, 6)          AS transactions_yoy,
       round(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed')
             / lag(sum(amount_egp) FILTER (WHERE transaction_status = 'Completed')) OVER (ORDER BY 1) - 1, 6)
                                                                                 AS value_yoy
FROM gold.fact_transaction
GROUP BY 1
ORDER BY 1;

-- 3. Status breakdown
SELECT transaction_status, count(*) AS transactions
FROM gold.fact_transaction GROUP BY 1 ORDER BY 2 DESC;

-- 4. Top 3 banks by completed value
SELECT b.bank_name,
       count(*)                                                                    AS transactions,
       round(sum(f.amount_egp) FILTER (WHERE f.transaction_status = 'Completed'), 2) AS transaction_value_egp,
       round(avg((f.transaction_status = 'Failed')::int), 6)                       AS failure_rate
FROM gold.fact_transaction f JOIN gold.dim_bank b USING (bank_id)
GROUP BY 1 ORDER BY 3 DESC LIMIT 3;

-- 5. Digital share by customer segment
SELECT c.customer_segment, round(avg(f.digital_channel_flag::int), 6) AS digital_share
FROM gold.fact_transaction f JOIN gold.dim_customer c USING (customer_id)
GROUP BY 1 ORDER BY 1;

-- 6. Data quality (semantic.dq_runs and dq_rule_checks; definitions of
--    dbt/models/semantic/_metrics.yml)
SELECT sum(processed)                                                    AS processed_records,
       sum(records_with_issues)                                          AS records_with_issues,
       round(sum(valid)::numeric / sum(processed), 6)                    AS initial_dq_score,
       round(sum(clean_records)::numeric / sum(processed), 6)            AS final_dq_score,
       round(sum(auto_fixed_records)::numeric / sum(records_with_issues), 6) AS auto_fix_rate,
       round(sum(resolved)::numeric / sum(quarantined), 6)               AS quarantine_resolution_rate,
       sum(unresolved_records)                                           AS unresolved_records
FROM semantic.dq_runs;

SELECT sum(checked) AS values_checked, sum(failed) AS values_failed, sum(repaired) AS values_repaired,
       round(sum(failed)::numeric / sum(checked), 6)   AS rule_failure_rate,
       round(sum(repaired)::numeric / sum(failed), 6)  AS rule_repair_rate
FROM semantic.dq_rule_checks;
