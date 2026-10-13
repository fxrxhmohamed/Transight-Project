-- Checks of the initial and incremental loads (bronze -> silver -> gold).
-- Every check returns one row: its name, the problems found and PASS/FAIL.
--
--   docker exec -i transight-project-postgres-1 psql -U metaflow -d metaflow < sql/checks/load_checks.sql

\echo '== Loads into silver (dq.load_log) =='
SELECT load_id, run_id, mode, staged, inserted, updated, unchanged, older, removed,
       silver_rows_before AS before, silver_rows_after AS after,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI') AS loaded_at
FROM dq.load_log ORDER BY load_id;

\echo '== Checks (problems must be 0) =='
WITH checks(check_name, problems) AS (
    SELECT '1 bronze: same file ingested more than once (after the guard)',
           count(*) FROM (
               SELECT sha256 FROM bronze.ingestion
               WHERE ingested_at > (SELECT coalesce(min(loaded_at), now()) FROM dq.load_log)
               GROUP BY dataset, sha256 HAVING count(*) > 1) d
    UNION ALL
    SELECT '2 silver: duplicate transaction_id',
           (SELECT count(*) - count(DISTINCT transaction_id) FROM silver.banking_transactions)
    UNION ALL
    SELECT '3 silver: empty transaction_id',
           (SELECT count(*) FROM silver.banking_transactions WHERE transaction_id IS NULL)
    UNION ALL
    SELECT '4 silver: duplicate record_id',
           (SELECT count(*) - count(DISTINCT record_id) FROM silver.banking_transactions)
    UNION ALL
    SELECT '5 silver: row without its bronze record',
           (SELECT count(*) FROM silver.banking_transactions s
            LEFT JOIN bronze.banking_transactions b USING (record_id)
            WHERE b.record_id IS NULL)
    UNION ALL
    SELECT '6 silver: key or run differs from its bronze record',
           (SELECT count(*) FROM silver.banking_transactions s
            JOIN bronze.banking_transactions b USING (record_id)
            WHERE b.transaction_id IS DISTINCT FROM s.transaction_id
               OR b.ingestion_id <> s.run_id)
    UNION ALL
    SELECT '7 silver: valid/resolved key of a run missing from silver',
           (SELECT count(*) FROM (
                SELECT DISTINCT transaction_id FROM dq.banking_transactions_valid
                WHERE transaction_id IS NOT NULL
                  AND run_id IN (SELECT run_id FROM dq.load_log)
                EXCEPT SELECT transaction_id FROM silver.banking_transactions) m)
    UNION ALL
    SELECT '8 load log: counts do not add up',
           (SELECT count(*) FROM dq.load_log
            WHERE staged <> inserted + updated + unchanged + older
               OR silver_rows_after <> silver_rows_before + inserted - removed)
    UNION ALL
    SELECT '9 load log: last load does not match silver now',
           (SELECT count(*) FROM (SELECT silver_rows_after FROM dq.load_log
                                  ORDER BY load_id DESC LIMIT 1) l
            WHERE l.silver_rows_after <> (SELECT count(*) FROM silver.banking_transactions))
    UNION ALL
    SELECT '10 gold: fact rows differ from silver rows',
           (SELECT abs((SELECT count(*) FROM gold.fact_transaction)
                     - (SELECT count(*) FROM silver.banking_transactions)))
    UNION ALL
    SELECT '11 gold: duplicate transaction_id in the fact table',
           (SELECT count(*) - count(DISTINCT transaction_id) FROM gold.fact_transaction)
)
SELECT check_name, problems, CASE WHEN problems = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM checks ORDER BY split_part(check_name, ' ', 1)::int;

\echo '== Silver now =='
SELECT count(*) AS rows, count(DISTINCT transaction_id) AS transactions,
       count(DISTINCT run_id) AS runs_represented
FROM silver.banking_transactions;
