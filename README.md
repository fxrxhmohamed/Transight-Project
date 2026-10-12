# MetaFlow-DQ

Metadata-driven data quality and ETL for the Egypt banking transactions
dataset.

What the data should look like (columns, types, allowed values, ranges,
relationships) is described in a schema registry, and how strictly it is
enforced and repaired in a rules file. The pipeline lands each source file
unchanged, validates and repairs it, holds back the records it cannot
trust, lets people correct them (in the browser or with a file), merges
the result into a clean dataset and reports data-quality metrics for
Power BI. Every record keeps one id from ingestion to the final dataset.
Everything is stored in PostgreSQL; the only files are the source CSV and
the correction templates people fill in.

```
source CSV
  -> ingest       bronze.<dataset>, ingestion id
  -> profile      profile stored with the ingestion
  -> validate     dq.<dataset>_valid | dq.<dataset>_quarantine (+ why) | repair audit
  -> correct      correction UI or a correction file   <- people
  -> revalidate   fixed records resolved, the rest back to quarantine
  -> merge        silver.<dataset> (valid + resolved)
  -> metrics      metrics.dq_run, dq_rule_result, dq_quarantine, dq_correction (dbt)
  -> metadata     meta.* (what silver holds: entities, keys, relationships)
  -> gold         gold.fact_* / dim_* star schema, then marts.* (dbt)
```

## Project structure

```
MetaFlow-DQ/
├── dq.py                        one command per pipeline step (python dq.py --help)
├── docker-compose.yml           PostgreSQL + Airflow for local use
├── .env.example                 database settings (copy to .env)
│
├── config/
│   ├── metadata/schema_registry.yaml   columns, types, allowed values, entities
│   ├── rules/quality_rules.yaml        severities, repairs, business rules
│   └── reference/                      lookup files (banks, branches)
│
├── sql/
│   ├── 01_bronze.sql            bronze schema (ingestion catalog, append-only)
│   └── 02_dq.sql                dq, silver and metrics schemas
│
├── services/
│   ├── ingestion-service/src/   ingest.py   load a CSV into bronze
│   │                            bronze.py   bronze database client (shared)
│   ├── metadata-service/src/    profiler.py profile an ingestion
│   └── dq-service/src/
│       ├── dq_engine.py         validate: valid | quarantine | repair audit
│       ├── registry.py          load the schema registry and rules
│       ├── checks.py            rule checks
│       ├── remediation.py       automatic repairs
│       ├── values.py            typed values and canonical formats
│       ├── quarantine.py        correction template, import, re-validation
│       ├── merge.py             valid + resolved -> silver.<dataset>
│       ├── correction_ui.py     Streamlit correction UI
│       ├── correction_assist.py wording and suggestions for the UI
│       ├── layout.py            which table holds which output of a run
│       ├── db.py                PostgreSQL access (transactions, COPY)
│       └── import_files.py      load runs kept in data/ by earlier versions
│
├── dbt/                         metric tables and the MetricFlow semantic layer
│   ├── models/dq/               metrics.* from the dq schema (+ DQ metrics)
│   ├── models/business/         marts.* from gold (+ business metrics)
│   ├── models/semantic/         time spine for MetricFlow
│   └── requirements.txt         dbt-core, dbt-postgres, dbt-metricflow
│
├── airflow/
│   ├── Dockerfile               Airflow image with the pipeline's Python
│   └── dags/data_quality.py     DAGs, one dq.py step per task
│
├── tests/
│   ├── test_pipeline.py         end-to-end tests of every dq.py step
│   └── data/                    50-row test dataset and its expected defects
│
├── .github/workflows/           CI: the tests against PostgreSQL
└── data/raw/banking/            source files to ingest (local, not in git)
```

Each service has its own `Dockerfile` and `requirements.txt`. Records are
identified by `<ingestion id>_ROW_<row number>` from ingestion to the
clean data.

## Setup

Python 3.13 (what CI tests with). From the project root:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      Linux/macOS: source .venv/bin/activate
python -m pip install -r services/dq-service/requirements.txt \
                      -r services/metadata-service/requirements.txt

# dbt and MetricFlow (metrics step) in a virtual environment of their own:
python -m venv .venv-dbt
.venv-dbt/bin/pip install -r dbt/requirements.txt   # Windows: .venv-dbt\Scripts\pip
# then point dq.py at it: DBT=.venv-dbt/bin/dbt (or put it on the PATH)
```

The Docker images have both already (`/opt/dbt`, with `DBT` set).

The data is not in the repository. Put the source file at
`data/raw/banking/banking_transactions.csv`. Ingestion loads it into the
bronze layer in PostgreSQL (below), and every later step reads and writes
the database too.

## Database (PostgreSQL)

### Bronze

`sql/01_bronze.sql` creates the ingestion catalog; the first ingestion of
a dataset creates its table:

| Table | One row per |
|---|---|
| `bronze.ingestion` | ingestion: source file, SHA-256, row count, columns, record id format, time, profile (JSON) |
| `bronze.<dataset>` (e.g. `bronze.banking_transactions`) | source row, laid out like the CSV: `ingestion_id`, `row_number`, `record_id`, then one `text` column per source column with the value exactly as read |

All are append-only: triggers refuse updates and deletes (only the
profile of an ingestion may be filled in). A later ingestion with new
columns adds them to the table (NULL for earlier rows).
`bronze.ingestion_summary` lists the ingestions and the table holding each.

### Data quality, silver and metrics

`sql/02_dq.sql` creates the `dq`, `silver` and `metrics` schemas. Record
tables are created per dataset, with `record_id` and one `text` column per
data column (values as the engine cleaned them):

| Table | One row per |
|---|---|
| `dq.run` | validated run (ingestion): its record columns |
| `dq.<dataset>_valid` | record that passed the first validation, with its warnings |
| `dq.<dataset>_quarantine` | record open in a round, with its cleaned values |
| `dq.quarantine_error` | failed rule, record and round: code, severity, column, value, message |
| `dq.quarantine_status` | quarantined record: status, round, attempts (the ledger) |
| `dq.quarantine_history` | status change of a quarantined record |
| `dq.remediation_audit` | automatic repair: column, before, after, rule, outcome |
| `dq.correction` | manual correction to apply when its round is re-validated |
| `dq.correction_log` | manual correction ever imported, with who and when |
| `dq.import_rejection` | correction line that was refused, with the reason |
| `dq.<dataset>_resolved` | record that passed re-validation, per round |
| `dq.round_result` | round of a run: its summary (JSON) |
| `silver.<dataset>` | record of the clean data: valid + resolved, per run |
| `dq.merge_result` | run: counts and checks of its latest merge |
| `metrics.*` | see [Data quality metrics](#data-quality-metrics) |

Each step writes its tables in one transaction, so a step that fails
leaves nothing half written. Older database volumes get the `dq` tables
on the first run of a step.

### Setting it up

With Docker (creates the database and runs `sql/` on its first start):

```bash
cp .env.example .env              # optional: change the password here
docker compose up -d postgres     # localhost:5432
```

Without Docker, on an installed PostgreSQL 14 or later:

```bash
psql -U postgres -c "CREATE ROLE metaflow LOGIN CREATEDB PASSWORD 'change_me_local_only'"
psql -U postgres -c "CREATE DATABASE metaflow OWNER metaflow"
psql -U metaflow -d metaflow -f sql/01_bronze.sql
psql -U metaflow -d metaflow -f sql/02_dq.sql
```

The scripts connect with `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`,
`POSTGRES_USER` and `POSTGRES_PASSWORD` (environment, or `.env`, which
`dq.py` reads; defaults as in `.env.example`), or with `METAFLOW_DB_URL`
if set.

```sql
SELECT * FROM bronze.ingestion_summary;
SELECT record_id, transaction_id FROM bronze.banking_transactions
WHERE ingestion_id = 'ING_20261012_001' ORDER BY row_number LIMIT 10;

SELECT status, round, count(*) FROM dq.quarantine_status
WHERE run_id = 'ING_20261012_001' GROUP BY 1, 2;
SELECT * FROM silver.banking_transactions LIMIT 100;
```

### Runs from earlier versions

Earlier versions wrote the outputs to CSV and JSON files under `data/`.
Load those runs (with their rounds, ledger history and corrections) into
the database once, then rebuild the metrics:

```bash
python dq.py import-files            # every run under data/; --run <id> for one
python dq.py metrics
```

Runs already in the database are skipped. The files are left as they
are; delete `data/validated`, `quarantine`, `corrected`, `quality`,
`final` and `metrics` once the import is checked.

## Run the pipeline

Every step is one command of `dq.py`, run from the project root. Each
calls the service script that does the work (`python dq.py --help`).

```bash
# 1. Ingest: load into bronze with an ingestion id and record ids
#    (the last output line is the new ingestion id)
python dq.py ingest --input data/raw/banking/banking_transactions.csv

# 2. Profile the ingestion (stored in bronze.ingestion.profile)
python dq.py profile

# 3-4. Validate: valid records, quarantine round 1 with one error line per
#      failed rule, audit of every automatic repair
python dq.py validate

# 5. Correct: export the template, fill in new_value / action / reason,
#    import it (or use the correction UI below)
python dq.py export-template --output fixes.csv
python dq.py import-corrections --file fixes.csv

# 6. Re-validate the corrections against the whole dataset; repeat 5-6
python dq.py revalidate
python dq.py status

# 7. Merge into the clean dataset (silver.<dataset>): valid + resolved,
#    one row per record
python dq.py merge

# 8. Data-quality metric tables for Power BI (metrics schema, all runs)
python dq.py metrics

# Steps 1, 2, 3-4, 7 and 8 in one go
python dq.py pipeline --input data/raw/banking/banking_transactions.csv
```

Every step works on the latest ingestion unless given `--run <ingestion
id>`, and takes `--rules` to use another rules file. `validate --force`
starts a run over (it removes that run's quarantine rounds, corrections
and clean data).

### Correction file

One line per field to check. Fill in only these columns:

| To... | `new_value` | `action` |
|---|---|---|
| correct a field | the right value | empty or `FIX` |
| empty a field on purpose | `__NULL__` | empty or `FIX` |
| remove the whole record | empty | `DISCARD` |
| accept the automatic repairs of a record held for review | empty | `ACCEPT` |
| leave it for later | empty | empty |

`reason` is optional and kept in the correction log. Lines that are not
filled in are ignored; lines that cannot be used are stored with the
reason in `dq.import_rejection`. Without `--output`, `export-template`
writes `correction_template_<run>_round_<n>.csv` in the project folder.

## Correction UI

Steps 5-6 in the browser, for people who do not know the rules:

```bash
python dq.py ui
```

- **Fix records**: problems in plain words ("Missing email", "Duplicate
  transaction ID"). Missing values that other records of the same
  customer or account already have are suggested, and a quick fix adds
  every record they complete. Other records are shown one at a time,
  with inputs that only accept valid values, a side-by-side view for
  duplicates and a Remove button.
- **Many values changed automatically**: a record repaired automatically
  3 or more times (rule `DQ-REV-001`) is held until someone accepts the
  changes, corrects one of them or removes the record. Records with no
  other problem can be accepted in bulk after checking examples.
- **My changes**: nothing is written until you send. Sending runs the
  same import and re-validation as the commands, so the result is
  identical to uploading the same fixes as a file.
- **Progress**: fixed, removed, waiting, needing an expert.
  **Publish to clean data** runs the merge and the metrics; fixed records
  reach `silver.<dataset>` only when published (e.g. once a day).
- **Upload a file**: download the list of problems, fill it in (e.g. in
  Excel), upload it; it is checked before anything is sent.

The wording and suggestions come from the schema registry
(`correction_assist.py`), so they follow the dataset without code changes.

## Airflow

`airflow/dags/data_quality.py`; every task runs one `dq.py` step.

| DAG | Tasks | When |
|---|---|---|
| `dq_ingest_validate` | ingest, profile, validate, merge, metrics, metadata, gold | a new file; param `input` |
| `dq_apply_corrections` | import-corrections, revalidate, merge, metrics, metadata, gold | a filled-in correction file; params `run`, `file` |
| `dq_publish` | merge, metrics, metadata, gold | every day at 18:00 (records fixed in the UI) |

Locally with Docker (Airflow 3, no login, for development only):

```bash
docker compose --progress plain build    # first time: several minutes
docker compose up -d                     # http://localhost:8080
docker compose down                      # stop; data stays in the
                                         # postgres-data volume
```

The project folder is mounted at `/opt/metaflow` (code, config, source
files), and the DAGs use the same database as the commands and the UI.
The pipeline runs in its own
virtual environment inside the image, dbt in another (`/opt/dbt`).
Elsewhere, Airflow workers need `METAFLOW_HOME` (the project folder),
`METAFLOW_PYTHON` (a Python with the requirements above), `DBT` (the dbt
executable) and the `POSTGRES_*` variables.

## Tests and CI

`tests/data/banking_sample.csv` has 50 rows: clean ones and one example of
each defect, listed in `tests/data/expected_defects.csv` with the rule
that must catch it and the expected outcome (repaired, flagged,
quarantined or removed). `tests/test_pipeline.py` runs every `dq.py` step
on it, including a correction round, the merge, the metrics and the UI,
and checks each defect.

```bash
python -m pip install pytest
docker compose up -d postgres     # the tests need PostgreSQL
python -m pytest tests
```

The tests create a temporary database (`metaflow_test_*`) and drop it at
the end, so the user in `.env` needs `CREATEDB`. GitHub Actions runs the
same tests, with a PostgreSQL service, on every push and pull request
(`.github/workflows/data-quality.yml`), after checking the dbt project and
its semantic layer (`dbt parse`, `mf validate-configs`). `tests/data/build_sample.py`
rebuilds the dataset from a validated run.

## Configuration

| File | Content |
|---|---|
| `config/metadata/schema_registry.yaml` | columns, types, allowed values, ranges, entities and relationships |
| `config/rules/quality_rules.yaml` | severities, automatic repairs, business and cross-row rules |
| `config/reference/` | lookup files for referential checks (banks, branches) |

Severity decides what happens to a record that still fails after
repair: `critical` -> quarantine, `warning` -> accepted and flagged,
`info` -> counted only.

## Data quality metrics

The metric tables are dbt models (`dbt/`). `python dq.py metrics` runs
`dbt build --select tag:dq`: it rebuilds four tables in the `metrics`
schema for every run, from the `dq` schema, and tests them (each table is
swapped in whole, so readers never see a half-built one). Connect Power
BI to them:

| Table | One row per |
|---|---|
| `dq_run` | run and round: processed, valid, auto-fixed, quarantined, resolved, manually fixed, confirmed after review, discarded, unresolved, DQ scores |
| `dq_rule_result` | run, round and rule: checked, failed, repaired, unresolved |
| `dq_quarantine` | status change of a quarantined record |
| `dq_correction` | automatic repair or manual correction, with its outcome |

- initial DQ score = valid / processed
- final DQ score = (valid + resolved) / processed
- records with issues = auto-fixed + quarantined; the quarantined ones end
  up manually fixed, confirmed after review, resolved otherwise,
  discarded or unresolved

The latest round of a run gives the funnel directly, e.g. 350 with
issues -> 200 auto-fixed -> 130 manually fixed -> 20 unresolved
(`is_latest_round` marks it).

### Business marts

`python dq.py gold` builds the gold star schema, then runs `dbt build
--select tag:gold`: views over gold in the `marts` schema
(`fct_transactions`, `dim_banks`, `dim_branches`, `dim_customers`) and
`marts.transaction_monthly`, transactions per month, bank, channel, type
and status (counts, amounts in EGP, channel cost) for Power BI. These
models are written for the banking_transactions dataset.

### Semantic layer (MetricFlow)

The metrics themselves are defined once, in dbt's semantic layer
(`dbt/models/dq/_dq_semantic.yml`, `dbt/models/business/_business.yml`):

| Metric | Definition |
|---|---|
| `dq_initial_score`, `dq_final_score` | valid / processed, (valid + resolved) / processed |
| `dq_auto_fix_rate`, `dq_resolution_rate` | auto-fixed / records with issues, resolved / quarantined |
| `dq_rule_failure_rate`, `dq_rule_repair_rate` | failed / checked, repaired / failed (per rule, dimension, severity) |
| `dq_corrections`, `dq_manual_corrections`, ... | counts of the record funnel and the corrections |
| `transactions`, `transaction_amount_egp`, `average_transaction_egp` | count, sum and average of the transactions |
| `failure_rate`, `digital_share`, `cost_per_transaction_egp` | failed / all, digital channel / all, channel cost / transaction |
| `active_customers`, `digital_migration_opportunities` | distinct customers, flagged transactions |

Query them with MetricFlow, from `dbt/` (in the pipeline container:
`docker compose run --rm -w /opt/metaflow/dbt pipeline mf query ...`):

```bash
dbt parse --profiles-dir .     # once, and after changing the YAML: writes target/
mf query --metrics dq_initial_score,dq_final_score --group-by run_round__run_id
mf query --metrics dq_rule_failure_rate --group-by rule_result__dimension
mf query --metrics transactions,failure_rate --group-by metric_time__month,bank__bank_name
mf query --metrics active_customers --group-by customer__customer_segment
mf list metrics
```

Power BI reads the tables above; the semantic layer API for BI tools is
part of dbt Cloud only.
