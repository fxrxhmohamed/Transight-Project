"""Airflow DAGs for the data-quality pipeline.

Every task runs one step of dq.py (python dq.py <step> ...), so a task
does in Airflow exactly what the same command does by hand.

    dq_ingest_validate    ingest -> profile -> validate -> merge -> metrics
                          -> metadata -> gold -> semantic -> powerbi
                          A new source file: land it, check it, publish the
                          valid records (incremental load into silver).
                          Triggered by the correction UI ("Load new data",
                          files saved under data/raw/uploads/), or by hand
                          with {"input": "<csv path>"}. One run at a time.
    dq_apply_corrections  import-corrections -> revalidate -> merge -> metrics
                          -> metadata -> gold -> semantic -> powerbi
                          A filled-in correction file for a run. Triggered by
                          the correction UI when someone sends an uploaded
                          file (saved under data/corrections/), or by hand with
                          {"run": "<ingestion id>", "file": "<csv path>",
                          "user": "<who corrected>"}. One run at a time, so
                          two uploads are applied one after the other.
    dq_publish            merge -> metrics -> metadata -> gold -> semantic
                          -> powerbi, every evening
                          Puts the records fixed in the correction UI that
                          day into the clean data and the gold star schema.
                          Also triggered by "Publish to clean data" in the
                          correction UI with {"run": "<ingestion id>",
                          "user": "<who published>"}. One run at a time.

The data work of the steps runs on Spark: the tasks submit their jobs to
the cluster named by SPARK_MASTER (docker-compose.yml). metrics loads
the DQ tables of the semantic layer (dbt: semantic.dq_*); semantic
computes the business and DQ metrics (dbt + MetricFlow) and stores them in
the semantic schema; powerbi refreshes the Power BI semantic model from PostgreSQL, so
the reports show the run's data when the run ends. Without the POWERBI_*
settings (.env) it exits with 99 and Airflow marks it skipped.

Environment of the Airflow workers
    METAFLOW_HOME    project folder holding dq.py and config/
                     (default /opt/metaflow)
    METAFLOW_PYTHON  Python with the services' requirements and PySpark
                     (default python)
    SPARK_MASTER     spark://spark-master:7077, or local[*]
    POSTGRES_*       the database every step reads and writes
    DQ_ALERT_EMAIL,  e-mail on every run: success, or the failed task with
    SMTP_*           its error (dq_notify.py; unset: no e-mail)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import dq_notify

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, Param
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator


HOME = os.environ.get("METAFLOW_HOME", "/opt/metaflow")
PYTHON = os.environ.get("METAFLOW_PYTHON", "python")

DEFAULT_ARGS = {
    "owner": "data-quality",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # E-mail when a task fails for good (after its retry).
    "on_failure_callback": dq_notify.task_failed,
}


def dq(step: str, arguments: str = "") -> str:
    """Shell command for one dq.py step."""

    return f'cd "{HOME}" && {PYTHON} dq.py {step} {arguments}'.strip()


def task(task_id: str, step: str, arguments: str = "", **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=dq(step, arguments), **kwargs)


# The ingestion id printed by the ingest task (its last output line).
INGESTED = "{{ ti.xcom_pull(task_ids='ingest') }}"


with DAG(
    dag_id="dq_ingest_validate",
    description="Land a source file, profile and validate it, publish the valid records.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # Triggered by the correction UI: a paused DAG would leave its runs
    # queued. One file at a time, loaded in the order they were sent.
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_success_callback=dq_notify.dag_succeeded,
    params={"input": Param("data/raw/banking/banking_transactions.csv", type="string",
                           description="Source CSV, relative to METAFLOW_HOME.")},
    tags=["data-quality"],
):
    # A file already in bronze exits with 99: BashOperator's skip code, so
    # the task and the rest of the run are skipped (nothing new to load).
    # The path may come from the UI: passed in the environment, not pasted
    # into the shell command.
    ingest = task("ingest", "ingest", '--input "$DQ_INPUT"', do_xcom_push=True,
                  skip_on_exit_code=99, env={"DQ_INPUT": "{{ params.input }}"},
                  append_env=True)
    profile = task("profile", "profile", f"--run {INGESTED}")
    validate = task("validate", "validate", f"--run {INGESTED}")
    merge = task("merge", "merge", f"--run {INGESTED}")
    metrics = task("metrics", "metrics")
    metadata = task("metadata", "metadata")
    gold = task("gold", "gold")
    semantic = task("semantic", "semantic")
    powerbi = task("powerbi", "powerbi-refresh")

    ingest >> profile >> validate >> merge >> metrics >> metadata >> gold >> semantic >> powerbi


with DAG(
    dag_id="dq_apply_corrections",
    description="Import a filled-in correction file, re-validate and publish.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # Triggered by the correction UI: a paused DAG would leave its runs queued.
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_success_callback=dq_notify.dag_succeeded,
    params={
        "run": Param("latest", type="string", description="Ingestion id."),
        "file": Param("", type="string",
                      description="Filled-in correction template (CSV), "
                                  "relative to METAFLOW_HOME."),
        "user": Param("airflow", type="string",
                      description="Who made the corrections (correction log)."),
    },
    tags=["data-quality"],
):
    run = '--run "{{ params.run }}"'
    # The file and the name come from the UI: passed in the environment, not
    # pasted into the shell command. Not retried: a second import of the same
    # file would only be refused.
    imported = task("import_corrections", "import-corrections",
                    f'{run} --file "$DQ_FILE" --user "$DQ_USER"',
                    env={"DQ_FILE": "{{ params.file }}", "DQ_USER": "{{ params.user }}"},
                    append_env=True, retries=0)
    revalidated = task("revalidate", "revalidate", run)
    merged = task("merge", "merge", run)
    measured = task("metrics", "metrics")
    described = task("metadata", "metadata")
    built = task("gold", "gold")
    computed = task("semantic", "semantic")
    refreshed = task("powerbi", "powerbi-refresh")

    imported >> revalidated >> merged >> measured >> described >> built >> computed >> refreshed


with DAG(
    dag_id="dq_publish",
    description="Publish the records fixed in the correction UI: merge, metrics, gold.",
    start_date=datetime(2026, 1, 1),
    schedule="0 18 * * *",
    catchup=False,
    # Triggered by the correction UI too: a paused DAG would leave its runs
    # queued.
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_success_callback=dq_notify.dag_succeeded,
    params={
        "run": Param("latest", type="string", description="Ingestion id."),
        "user": Param("", type="string",
                      description="Who published (empty: the evening schedule)."),
    },
    tags=["data-quality"],
):
    # Run and name come from the UI: passed in the environment, not pasted
    # into the shell command.
    merged = task("merge", "merge", '--run "$DQ_RUN" --published-by "$DQ_USER"',
                  env={"DQ_RUN": "{{ params.run }}", "DQ_USER": "{{ params.user }}"},
                  append_env=True)
    (
        merged >> task("metrics", "metrics") >> task("metadata", "metadata")
        >> task("gold", "gold") >> task("semantic", "semantic")
        >> task("powerbi", "powerbi-refresh")
    )
