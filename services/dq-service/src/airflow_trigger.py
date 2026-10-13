"""Trigger Airflow DAGs from the correction UI (Airflow 3 REST API).

    AIRFLOW_API_URL      where the UI reaches Airflow, e.g. http://airflow:8080
                         (docker-compose.yml); unset: no Airflow, the UI does
                         the work itself
    AIRFLOW_UI_URL       where people open Airflow in their browser, for the
                         link to a run (default: AIRFLOW_API_URL)
    AIRFLOW_USERNAME,    an Airflow login, when Airflow asks for one; without
    AIRFLOW_PASSWORD     them a token is requested without credentials (the
                         no-login mode of docker-compose.yml)

Only the standard library: the UI needs no extra package for it.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from urllib.parse import quote

TIMEOUT = 30


class AirflowError(RuntimeError):
    pass


def api_url() -> str | None:
    url = os.environ.get("AIRFLOW_API_URL", "").strip()
    return url.rstrip("/") or None


def enabled() -> bool:
    return api_url() is not None


def run_link(dag_id: str, run_id: str) -> str | None:
    """Browser link to a DAG run."""

    base = (os.environ.get("AIRFLOW_UI_URL", "").strip() or api_url() or "").rstrip("/")
    return f"{base}/dags/{dag_id}/runs/{quote(run_id, safe='')}" if base else None


def _call(method: str, path: str, body: dict | None = None,
          token: str | None = None) -> dict:
    base = api_url()
    if base is None:
        raise AirflowError("AIRFLOW_API_URL is not set.")
    request = urllib.request.Request(
        base + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise AirflowError(f"Airflow answered {error.code} to {method} {path}: "
                           f"{detail}") from error
    except urllib.error.URLError as error:
        raise AirflowError(f"Airflow cannot be reached at {base}: "
                           f"{error.reason}") from error


def _token() -> str:
    user = os.environ.get("AIRFLOW_USERNAME")
    password = os.environ.get("AIRFLOW_PASSWORD")
    if user and password:
        answer = _call("POST", "/auth/token", {"username": user, "password": password})
    else:
        answer = _call("GET", "/auth/token")
    token = answer.get("access_token")
    if not token:
        raise AirflowError("Airflow did not return an access token.")
    return token


def trigger(dag_id: str, conf: dict) -> str:
    """Start a run of the DAG with these params; returns its run id.

    Unpauses the DAG first: Airflow keeps the runs of a paused DAG queued.
    """

    token = _token()
    _call("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False}, token=token)
    answer = _call("POST", f"/api/v2/dags/{dag_id}/dagRuns",
                   {"logical_date": None, "conf": conf}, token=token)
    return answer["dag_run_id"]


def run_state(dag_id: str, run_id: str) -> str:
    """queued, running, success or failed."""

    answer = _call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}",
                   token=_token())
    return answer["state"]
