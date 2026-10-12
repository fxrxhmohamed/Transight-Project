"""Spark session and PostgreSQL (JDBC) access shared by every step.

Session
    SPARK_MASTER          local[*] (default) or spark://spark-master:7077
    SPARK_DRIVER_MEMORY   default 4g (local mode runs everything in the driver)
    SPARK_EXECUTOR_MEMORY default 2g (cluster mode)
    SPARK_SHUFFLE_PARTITIONS  default 16 (adaptive execution merges small ones)
    SPARK_DRIVER_HOST     address executors use to reach the driver (cluster)
    METAFLOW_JDBC_JAR     PostgreSQL JDBC driver; default /opt/jars/postgresql.jar,
                          downloaded from Maven when that file is missing

The session runs with ANSI mode off (a value that does not parse becomes
null instead of failing the job), the CORRECTED date parser and UTC.
Every transformation uses Spark SQL functions only (no Python UDFs), and
rows built on the driver go through local_frame (Arrow, decoded by the
JVM), so executors never need Python: the cluster's workers run another
Python version than the driver.

Database: the same settings as bronze.connect (METAFLOW_DB_URL or the
POSTGRES_* variables).
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlparse

from typing import Any, Iterable, Sequence

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"
DEFAULT_JAR = "/opt/jars/postgresql.jar"
WRITE_BATCH = 10_000
WRITE_PARTITIONS = 8

_session: SparkSession | None = None


def session(app: str = "metaflow-dq") -> SparkSession:
    """The process's Spark session (created on first use)."""

    global _session
    if _session is not None:
        return _session

    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions",
                os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # local_frame: driver rows travel as Arrow. No silent fallback to
        # pickled Python rows, which executors cannot read.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        # Wide rule plans: keep the optimizer from re-analysing them for long.
        .config("spark.sql.optimizer.maxIterations", "200")
    )
    if os.environ.get("SPARK_DRIVER_HOST"):
        builder = builder.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])

    jar = os.environ.get("METAFLOW_JDBC_JAR", DEFAULT_JAR)
    if Path(jar).is_file():
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", JDBC_PACKAGE)

    _session = builder.getOrCreate()
    _session.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return _session


def local_frame(rows: Iterable[Sequence[Any]], schema: str) -> DataFrame:
    """Rows (tuples) built on the driver as a DataFrame with a DDL schema
    ("name type, ...").

    Use this instead of createDataFrame on a list: that ships the rows as
    pickled Python, which executors can only read with the driver's Python
    version (PYTHON_VERSION_MISMATCH on the cluster).
    """

    fields = [field.strip().split(None, 1) for field in schema.split(",")]
    rows = list(rows)
    if not rows:
        return session().range(0).selectExpr(
            *[f"CAST(NULL AS {kind}) AS `{name}`" for name, kind in fields])
    frame = pd.DataFrame(rows, columns=[name for name, _ in fields], dtype=object)
    return session().createDataFrame(frame, schema)


def stop() -> None:
    global _session
    if _session is not None:
        _session.stop()
        _session = None


# ============================================================
# JDBC
# ============================================================

def jdbc_options() -> dict[str, str]:
    """URL, user and password from METAFLOW_DB_URL or POSTGRES_*."""

    url = os.environ.get("METAFLOW_DB_URL")
    if url:
        parts = urlparse(url)
        host, port = parts.hostname or "localhost", parts.port or 5432
        database = parts.path.lstrip("/") or "metaflow"
        user, password = unquote(parts.username or ""), unquote(parts.password or "")
    else:
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        database = os.environ.get("POSTGRES_DB", "metaflow")
        user = os.environ.get("POSTGRES_USER", "metaflow")
        password = os.environ.get("POSTGRES_PASSWORD", "change_me_local_only")

    return {
        # stringtype=unspecified: text values are cast by PostgreSQL to the
        # column type (date, time, numeric, ...), as COPY would.
        "url": (f"jdbc:postgresql://{host}:{port}/{database}"
                "?reWriteBatchedInserts=true&stringtype=unspecified"),
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def read(
    query: str,
    predicates: list[str] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Rows of a SELECT. predicates splits the read into parallel parts
    (one WHERE condition each)."""

    spark = spark or session()
    options = jdbc_options()
    properties = {k: v for k, v in options.items() if k != "url"}
    table = f"({query}) AS q"
    if predicates:
        return spark.read.jdbc(options["url"], table, predicates=predicates,
                               properties=properties)
    return spark.read.format("jdbc").options(**options, dbtable=table).load()


def hash_predicates(column: str, parts: int = 8) -> list[str]:
    """WHERE conditions that split rows evenly on a text column."""

    return [f"abs(hashtext({column})) % {parts} = {i}" for i in range(parts)]


def write(df: DataFrame, table: str, partitions: int = WRITE_PARTITIONS) -> None:
    """Append rows to an existing table (columns matched by name)."""

    (
        df.repartition(partitions)
        .write.format("jdbc")
        .options(**jdbc_options(), dbtable=table, batchsize=str(WRITE_BATCH))
        .mode("append")
        .save()
    )
