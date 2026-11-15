from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BASE_DIR = Path(os.environ.get("METAFLOW_HOME", "/opt/metaflow"))

LANDING_DIR = BASE_DIR / "data" / "landing"
ARCHIVE_DIR = BASE_DIR / "data" / "archive"
FAILED_DIR = BASE_DIR / "data" / "failed"

PROCESSING_DAG_ID = "dq_ingest_validate"

FILE_STABILITY_SECONDS = 30

AIRFLOW_HELPER_DIR = (
    BASE_DIR / "services" / "dq-service" / "src"
)

if str(AIRFLOW_HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HELPER_DIR))


# ---------------------------------------------------------
# File Discovery
# ---------------------------------------------------------

def discover_file(**context):
    """Find the oldest stable CSV file."""

    if not LANDING_DIR.exists():
        print(f"Landing directory not found: {LANDING_DIR}")
        return None

    cutoff = (
        pendulum.now("UTC").timestamp()
        - FILE_STABILITY_SECONDS
    )

    candidates = []

    for file_path in LANDING_DIR.glob("*.csv"):

        if not file_path.is_file():
            continue

        try:
            if file_path.stat().st_mtime > cutoff:
                continue

            candidates.append(file_path)

        except OSError as error:
            print(f"Cannot inspect {file_path}: {error}")

    if not candidates:
        print("No eligible CSV files found.")
        return None

    selected_file = min(
        candidates,
        key=lambda path: path.stat().st_mtime,
    )

    relative_path = selected_file.relative_to(
        BASE_DIR
    ).as_posix()

    print(f"Selected file: {relative_path}")

    return relative_path


def has_file(**context):
    """Check whether a file was discovered."""

    return bool(
        context["ti"].xcom_pull(
            task_ids="discover_file"
        )
    )


# ---------------------------------------------------------
# Child DAG Failure Verification
# ---------------------------------------------------------

def verify_child_failure(**context):
    """
    Confirm that the child DAG actually failed.

    API or infrastructure errors must not be classified
    as data-quality failures.
    """

    ti = context["ti"]

    run_id = ti.xcom_pull(
        task_ids="trigger_processing",
        key="trigger_run_id",
    )

    if not run_id:
        raise RuntimeError(
            "Child DAG run ID is unavailable. "
            "Keeping the file in landing."
        )

    import airflow_trigger

    state = airflow_trigger.run_state(
        PROCESSING_DAG_ID,
        run_id,
    )

    print(f"Child run ID: {run_id}")
    print(f"Child run state: {state}")

    if state == "failed":
        return True

    raise RuntimeError(
        f"Child DAG state is {state}. "
        "Failure is not confirmed."
    )


# ---------------------------------------------------------
# File Movement
# ---------------------------------------------------------

def get_unique_destination(
    directory: Path,
    filename: str,
) -> Path:
    """Avoid overwriting existing files."""

    destination = directory / filename

    if not destination.exists():
        return destination

    stem = Path(filename).stem
    suffix = Path(filename).suffix

    counter = 1

    while True:

        destination = directory / (
            f"{stem}_{counter}{suffix}"
        )

        if not destination.exists():
            return destination

        counter += 1


def move_file(destination_dir: str, **context):
    """Move the discovered file to its destination."""

    ti = context["ti"]

    relative_path = ti.xcom_pull(
        task_ids="discover_file"
    )

    if not relative_path:
        raise RuntimeError("Discovered file path is missing.")

    source = BASE_DIR / relative_path

    if not source.is_file():
        raise FileNotFoundError(
            f"Source file not found: {source}"
        )

    destination_dir = Path(destination_dir)

    destination_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination = get_unique_destination(
        destination_dir,
        source.name,
    )

    shutil.move(
        str(source),
        str(destination),
    )

    print(f"File moved successfully: {destination}")


# ---------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------

with DAG(
    dag_id="file_watcher",
    description="Automatically discover and process CSV files.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "file-watcher"],
) as dag:

    discover = PythonOperator(
        task_id="discover_file",
        python_callable=discover_file,
    )

    check_file = ShortCircuitOperator(
        task_id="has_file",
        python_callable=has_file,
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id=PROCESSING_DAG_ID,
        conf={
            "input": "{{ ti.xcom_pull(task_ids='discover_file') }}"
        },
        wait_for_completion=True,
        deferrable=True,
        poke_interval=15,
    )

    archive_file = PythonOperator(
        task_id="archive_file",
        python_callable=move_file,
        op_kwargs={
            "destination_dir": str(ARCHIVE_DIR)
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    verify_failure = PythonOperator(
        task_id="verify_child_failure",
        python_callable=verify_child_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    failed_file = PythonOperator(
        task_id="failed_file",
        python_callable=move_file,
        op_kwargs={
            "destination_dir": str(FAILED_DIR)
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    discover >> check_file >> trigger_processing

    trigger_processing >> archive_file

    trigger_processing >> verify_failure
    verify_failure >> failed_file