"""Run dbt and MetricFlow on the project in dbt/ (the semantic layer).

Used by the metrics step (the DQ tables) and the semantic step (the
metrics). dbt and MetricFlow run from their own virtual environment: DBT
names the dbt executable (default: dbt on the PATH; the images set
/opt/dbt/bin/dbt), mf is taken from the same folder.

They run in a temporary copy of dbt/ (workspace), so their target/ and
logs/ are never shared: the project folder is mounted in containers running
as different users (pipeline and UI as root, Airflow as airflow), and two
runs never write the same files.

The database: the POSTGRES_* variables (dbt/profiles.yml), set from
METAFLOW_DB_URL when it is given.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlparse

DBT_PROJECT = Path(__file__).resolve().parents[3] / "dbt"


class DbtError(RuntimeError):
    """dbt or MetricFlow failed."""


def executables() -> tuple[str, str]:
    """dbt and mf."""

    dbt = os.environ.get("DBT", "dbt")
    if os.sep in dbt or "/" in dbt:
        return dbt, str(Path(dbt).with_name("mf"))
    return dbt, shutil.which("mf") or "mf"


@contextmanager
def workspace() -> Iterator[Path]:
    """A temporary copy of dbt/ (without target/ and logs/)."""

    with tempfile.TemporaryDirectory() as work:
        project = Path(work) / "dbt"
        shutil.copytree(DBT_PROJECT, project,
                        ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
        yield project


def environment(project: Path) -> dict[str, str]:
    env = dict(os.environ, DBT_PROFILES_DIR=str(project))
    url = env.get("METAFLOW_DB_URL")
    if url:
        parts = urlparse(url)
        env.update(
            POSTGRES_HOST=parts.hostname or "localhost",
            POSTGRES_PORT=str(parts.port or 5432),
            POSTGRES_DB=parts.path.lstrip("/") or "metaflow",
            POSTGRES_USER=unquote(parts.username or ""),
            POSTGRES_PASSWORD=unquote(parts.password or ""),
        )
    return env


def call(project: Path, *line: str) -> str:
    """Run dbt or mf in the project; its output. DbtError when it fails."""

    print("$ " + " ".join(Path(line[0]).name if i == 0 else a for i, a in enumerate(line)),
          flush=True)
    result = subprocess.run(line, cwd=project, env=environment(project),
                            capture_output=True, text=True)
    if result.returncode:
        raise DbtError(f"{Path(line[0]).name} failed:\n{result.stdout[-4000:]}"
                       f"\n{result.stderr[-2000:]}")
    return result.stdout
