"""E-mail the outcome of the data-quality DAG runs (data_quality.py).

    dag_succeeded   DAG callback: the run finished (skipped tasks, such as
                    powerbi without Power BI settings, count as success)
    task_failed     task callback: a task failed after its retries, with
                    the error and a link to its log; the tasks after it
                    are not run, so a failed run sends one e-mail

Settings (.env, passed to the airflow service by docker-compose.yml):

    DQ_ALERT_EMAIL   who gets the e-mails (several: comma-separated);
                     unset: nothing is sent
    SMTP_HOST        default smtp.gmail.com
    SMTP_PORT        default 587 (STARTTLS)
    SMTP_USER        the account that sends, e.g. a Gmail address
    SMTP_PASSWORD    its password; for Gmail an app password
                     (Google account -> Security -> 2-Step Verification
                     -> App passwords), not the account password
    SMTP_FROM        sender shown (default SMTP_USER)
    AIRFLOW_UI_URL   Airflow address for the links (default
                     http://localhost:8080)

A mail that cannot be sent is logged, never fails the run. Only the
standard library.
"""

from __future__ import annotations

import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
from typing import Any
from urllib.parse import quote

log = logging.getLogger(__name__)

PREFIX = "[Transight]"


def _recipients() -> list[str]:
    return [a.strip() for a in os.environ.get("DQ_ALERT_EMAIL", "").split(",") if a.strip()]


def send(subject: str, body: str) -> bool:
    """Send one e-mail to DQ_ALERT_EMAIL; False when not configured or it failed."""

    recipients = _recipients()
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()
    if not (recipients and user and password):
        log.info("E-mail not sent (DQ_ALERT_EMAIL, SMTP_USER or SMTP_PASSWORD not set): %s",
                 subject)
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = os.environ.get("SMTP_FROM", "").strip() or user
    message["To"] = ", ".join(recipients)
    message.set_content(body)
    try:
        with smtplib.SMTP(os.environ.get("SMTP_HOST", "smtp.gmail.com"),
                          int(os.environ.get("SMTP_PORT") or 587), timeout=30) as smtp:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(user, password)
            smtp.send_message(message)
    except (OSError, smtplib.SMTPException) as error:
        log.error("E-mail %r could not be sent: %s", subject, error)
        return False
    log.info("E-mail sent to %s: %s", message["To"], subject)
    return True


def _run_link(dag_id: str, run_id: str) -> str:
    base = (os.environ.get("AIRFLOW_UI_URL") or "http://localhost:8080").rstrip("/")
    return f"{base}/dags/{dag_id}/runs/{quote(run_id, safe='')}"


def _when(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
    return str(value) if value else "-"


def _duration(start: Any, end: Any) -> str:
    if isinstance(start, datetime) and isinstance(end, datetime):
        seconds = int((end - start).total_seconds())
        return f"{seconds // 3600}h {seconds % 3600 // 60}m {seconds % 60}s"
    return "-"


def _params(conf: Any) -> str:
    if not conf:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in dict(conf).items() if value != "")


def dag_succeeded(context: dict[str, Any]) -> None:
    run = context["dag_run"]
    end = getattr(run, "end_date", None) or datetime.now(tz=getattr(run.start_date, "tzinfo", None))
    send(
        f"{PREFIX} SUCCESS: {run.dag_id}",
        "\n".join([
            f"The data-quality DAG {run.dag_id} finished successfully.",
            "",
            f"Run:       {run.run_id}",
            f"Started:   {_when(run.start_date)}",
            f"Finished:  {_when(end)}",
            f"Duration:  {_duration(run.start_date, end)}",
            f"Params:    {_params(getattr(run, 'conf', None))}",
            "",
            "The clean data, the gold star schema and the semantic layer",
            "(semantic.metric_report) are up to date for Power BI.",
            "",
            f"Run in Airflow: {_run_link(run.dag_id, run.run_id)}",
        ]),
    )


def task_failed(context: dict[str, Any]) -> None:
    ti = context["ti"]
    run = context.get("dag_run")
    run_id = getattr(ti, "run_id", None) or getattr(run, "run_id", "-")
    error = str(context.get("exception") or "-")
    log_url = getattr(ti, "log_url", None) or _run_link(ti.dag_id, run_id)
    send(
        f"{PREFIX} FAILED: {ti.dag_id} (task {ti.task_id})",
        "\n".join([
            f"The data-quality DAG {ti.dag_id} failed at task {ti.task_id};",
            "the tasks after it did not run.",
            "",
            f"Run:      {run_id}",
            f"Task:     {ti.task_id} (try {getattr(ti, 'try_number', '-')})",
            f"Params:   {_params(getattr(run, 'conf', None))}",
            f"Error:    {error[-2000:]}",
            "",
            "The step's own message is in the task log (the lines before",
            "\"Command exited with return code\").",
            "",
            f"Task log: {log_url}",
            f"Run in Airflow: {_run_link(ti.dag_id, run_id)}",
        ]),
    )
