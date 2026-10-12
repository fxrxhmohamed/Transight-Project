"""Airflow DAGs for the data-quality pipeline.

Every task runs one step of dq.py (python dq.py <step> ...), so a task
does in Airflow exactly what the same command does by hand.

    dq_ingest_validate    ingest -> profile -> validate -> merge -> metrics
                          A new source file: land it, check it, publish the
                          valid records. Trigger with {"input": "<csv path>"}.
    dq_apply_corrections  import-corrections -> revalidate -> merge -> metrics
                          A filled-in correction file for a run. Trigger with
                          {"run": "<ingestion id>", "file": "<csv path>"}.
    dq_publish            merge -> metrics, every evening
                          Puts the records fixed in the correction UI that
                          day into the clean data (the UI's Publish button).

Environment of the Airflow workers
    METAFLOW_HOME    project folder holding dq.py and config/
                     (default /opt/metaflow)
    METAFLOW_PYTHON  Python with the services' requirements (default python)
    POSTGRES_*       the database every step reads and writes
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

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
    default_args=DEFAULT_ARGS,
    params={"input": Param("data/raw/banking/banking_transactions.csv", type="string",
                           description="Source CSV, relative to METAFLOW_HOME.")},
    tags=["data-quality"],
):
    ingest = task("ingest", "ingest", '--input "{{ params.input }}"', do_xcom_push=True)
    profile = task("profile", "profile", f"--run {INGESTED}")
    validate = task("validate", "validate", f"--run {INGESTED}")
    merge = task("merge", "merge", f"--run {INGESTED}")
    metrics = task("metrics", "metrics")

    ingest >> profile >> validate >> merge >> metrics


with DAG(
    dag_id="dq_apply_corrections",
    description="Import a filled-in correction file, re-validate and publish.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run": Param("latest", type="string", description="Ingestion id."),
        "file": Param("", type="string",
                      description="Filled-in correction template (CSV)."),
    },
    tags=["data-quality"],
):
    run = '--run "{{ params.run }}"'
    imported = task("import_corrections", "import-corrections",
                    f'{run} --file "{{{{ params.file }}}}" --user airflow')
    revalidated = task("revalidate", "revalidate", run)
    merged = task("merge", "merge", run)
    measured = task("metrics", "metrics")

    imported >> revalidated >> merged >> measured


with DAG(
    dag_id="dq_publish",
    description="Publish the records fixed in the correction UI: merge and metrics.",
    start_date=datetime(2026, 1, 1),
    schedule="0 18 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-quality"],
):
    task("merge", "merge", "--run latest") >> task("metrics", "metrics")
