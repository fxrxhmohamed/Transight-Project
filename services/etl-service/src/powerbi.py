"""Refresh the Power BI semantic model (dataset) after the semantic step.

The last task of every DAG: once gold, the DQ views and the semantic
layer are in PostgreSQL, Power BI is asked to import them again (Power BI
REST API), and the step waits for the refresh to finish, so a run ends
with the reports showing its data.

    POWERBI_TENANT_ID       Microsoft Entra tenant (directory) id
    POWERBI_CLIENT_ID       app registration (service principal) id
    POWERBI_CLIENT_SECRET   its client secret
    POWERBI_WORKSPACE_ID    workspace holding the semantic model
    POWERBI_DATASET_ID      the semantic model (dataset) id
    POWERBI_REFRESH_TIMEOUT seconds to wait for the refresh (default 1800)

In .env (dq.py reads it) or the environment. The service principal needs
Member access to the workspace, and the semantic model's PostgreSQL
source must be reachable by Power BI (an on-premises data gateway for a
local database). See README, "Power BI".

Exit code: 0 refreshed, 1 the refresh failed, 99 Power BI is not
configured (Airflow marks the task skipped: nothing to refresh).

Only the standard library.

Usage
-----
    python src/powerbi.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SETTINGS = ("POWERBI_TENANT_ID", "POWERBI_CLIENT_ID", "POWERBI_CLIENT_SECRET",
            "POWERBI_WORKSPACE_ID", "POWERBI_DATASET_ID")
API = "https://api.powerbi.com/v1.0/myorg"
SCOPE = "https://analysis.windows.net/powerbi/api/.default"
NOT_CONFIGURED = 99
POLL_SECONDS = 15
TIMEOUT = 60


class PowerBIError(RuntimeError):
    pass


def _request(url: str, method: str = "GET", data: bytes | None = None,
             headers: dict[str, str] | None = None) -> tuple[dict, dict[str, str]]:
    """The JSON answer (empty when there is none) and the response headers."""

    request = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            body = response.read()
            return (json.loads(body) if body else {}), dict(response.headers)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise PowerBIError(f"{method} {url} answered {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise PowerBIError(f"{url} cannot be reached: {error.reason}") from error


def token(settings: dict[str, str]) -> str:
    """An access token of the service principal (client credentials)."""

    form = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": settings["POWERBI_CLIENT_ID"],
        "client_secret": settings["POWERBI_CLIENT_SECRET"],
        "scope": SCOPE,
    }).encode()
    tenant = urllib.parse.quote(settings["POWERBI_TENANT_ID"], safe="")
    answer, _ = _request(f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
                         "POST", form,
                         {"Content-Type": "application/x-www-form-urlencoded"})
    if not answer.get("access_token"):
        raise PowerBIError("Microsoft Entra ID did not return an access token.")
    return answer["access_token"]


def refresh(settings: dict[str, str], timeout: float) -> str:
    """Start a refresh, wait for it; its final status (Completed, Failed, ...)."""

    headers = {"Authorization": f"Bearer {token(settings)}",
               "Content-Type": "application/json"}
    refreshes = (f"{API}/groups/{settings['POWERBI_WORKSPACE_ID']}"
                 f"/datasets/{settings['POWERBI_DATASET_ID']}/refreshes")

    _, answer_headers = _request(refreshes, "POST",
                                 json.dumps({"notifyOption": "NoNotification"}).encode(),
                                 headers)
    request_id = next((v for k, v in answer_headers.items() if k.lower() == "requestid"), None)
    print(f"Power BI refresh started (request {request_id or 'unknown'}).", flush=True)

    deadline = time.monotonic() + timeout
    while True:
        history, _ = _request(f"{refreshes}?$top=5", headers=headers)
        mine = [r for r in history.get("value", [])
                if request_id is None or r.get("requestId") == request_id]
        status = mine[0].get("status") if mine else "Unknown"
        if status != "Unknown":   # Unknown: still running
            if status != "Completed":
                detail = mine[0].get("serviceExceptionJson") or ""
                print(f"Power BI refresh {status}. {detail}".strip(), file=sys.stderr)
            return status
        if time.monotonic() > deadline:
            raise PowerBIError(f"the refresh did not finish within {timeout:.0f} s "
                               "(it may still finish in Power BI).")
        time.sleep(POLL_SECONDS)


def main() -> int:
    settings = {name: os.environ.get(name, "").strip() for name in SETTINGS}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        print("Power BI is not configured (missing " + ", ".join(missing) + "): "
              "nothing to refresh. Power BI reads PostgreSQL on its own refresh.")
        return NOT_CONFIGURED

    timeout = float(os.environ.get("POWERBI_REFRESH_TIMEOUT") or 1800)
    try:
        status = refresh(settings, timeout)
    except PowerBIError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    if status != "Completed":
        return 1
    print("Power BI semantic model refreshed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
