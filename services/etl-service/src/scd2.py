
"""Build observed SCD Type 2 history for Gold dimensions.

History is reconstructed from accepted DQ records:
    dq.<dataset>_valid
    dq.<dataset>_resolved

The Silver table is intentionally not used because it stores only the
latest state for each natural key.

Date semantics:
    valid_from = bronze.ingestion.ingested_at
    valid_to   = next observed change
    is_current = latest observed version

These are observed-history timestamps, not business-effective dates.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "services" / "common" / "src"))

import bronze
import spark_io

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def quote(name: str) -> str:
    """Safely quote a PostgreSQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _postgres_type(kind: str) -> str:
    """Map registry types to supported PostgreSQL cast types."""
    normalized = str(kind).strip().lower()

    allowed = {
        "text": "text",
        "string": "text",
        "varchar": "text",
        "character varying": "text",
        "integer": "integer",
        "int": "integer",
        "int4": "integer",
        "bigint": "bigint",
        "int8": "bigint",
        "smallint": "smallint",
        "boolean": "boolean",
        "bool": "boolean",
        "numeric": "numeric",
        "decimal": "numeric",
        "real": "real",
        "double": "double precision",
        "double precision": "double precision",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
        "timestamptz": "timestamptz",
    }

    if normalized not in allowed:
        raise ValueError(f"Unsupported PostgreSQL type: {kind}")

    return allowed[normalized]


def _record_tables(dataset: str) -> tuple[str, str]:
    """Return the valid and resolved DQ table names."""
    base_name = bronze.table_name_of(dataset)
    return f"{base_name}_valid", f"{base_name}_resolved"


def _clean_records_query(
    dataset: str,
    columns: list[str],
    types: dict[str, str],
) -> str:
    """Build a query containing accepted records and their ingestion time.

    Valid records are taken from the first validation pass.
    Resolved records use the latest correction round per record.
    Only records that were successfully merged are included.
    """

    valid_table, resolved_table = _record_tables(dataset)

    conn = bronze.connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass(%s), to_regclass(%s)",
                (
                    f"dq.{valid_table}",
                    f"dq.{resolved_table}",
                ),
            )
            valid_exists, resolved_exists = cursor.fetchone()
    finally:
        conn.close()

    if not valid_exists and not resolved_exists:
        raise RuntimeError(
            f"No DQ valid/resolved tables found for dataset '{dataset}'."
        )

    selected_columns = []
    for column in columns:
        if column not in types:
            raise ValueError(
                f"Missing registry type for column '{column}'."
            )

        pg_type = _postgres_type(types[column])
        selected_columns.append(
            f"CAST(r.{quote(column)} AS {pg_type}) AS {quote(column)}"
        )

    selected_sql = ",\n            ".join(selected_columns)

    accepted_queries = []

    if valid_exists:
        accepted_queries.append(
            f"""
            SELECT
                r.run_id,
                r.record_id,
                {selected_sql}
            FROM dq.{quote(valid_table)} AS r
            """
        )

    if resolved_exists:
        accepted_queries.append(
            f"""
            SELECT
                latest.run_id,
                latest.record_id,
                {", ".join(
                    f"CAST(latest.{quote(column)} AS {_postgres_type(types[column])}) "
                    f"AS {quote(column)}"
                    for column in columns
                )}
            FROM (
                SELECT DISTINCT ON (r.run_id, r.record_id)
                    r.*
                FROM dq.{quote(resolved_table)} AS r
                ORDER BY
                    r.run_id,
                    r.record_id,
                    r.round DESC
            ) AS latest
            """
        )

    accepted_sql = "\nUNION ALL\n".join(accepted_queries)

    key_column = columns[0]

    return f"""
        SELECT
            accepted.run_id,
            accepted.record_id,
            ingestion.ingested_at,
            {", ".join(
                f"accepted.{quote(column)}"
                for column in columns
            )}
        FROM (
            {accepted_sql}
        ) AS accepted
        INNER JOIN dq.merge_result AS merged
            ON merged.dataset = '{dataset.replace("'", "''")}'
            AND merged.run_id = accepted.run_id
        INNER JOIN bronze.ingestion AS ingestion
            ON ingestion.ingestion_id = accepted.run_id
        WHERE accepted.{quote(key_column)} IS NOT NULL
    """


def build_entity_history(
    dataset: str,
    entity: str,
    key: str,
    attributes: list[str],
    references: list[str],
    types: dict[str, str],
) -> DataFrame:
    """Build SCD2 history for one entity.

    Each entity version represents one distinct observed state per ingestion.

    When an entity has conflicting states within one ingestion, the most
    frequently observed complete state is selected. If multiple states tie
    for the highest frequency, the function raises an error.
    """

    columns = list(
        dict.fromkeys([key, *attributes, *references])
    )

    if not columns:
        raise ValueError(
            f"No columns supplied for entity '{entity}'."
        )

    missing_types = [
        column for column in columns
        if column not in types
    ]

    if missing_types:
        raise ValueError(
            f"Missing types for {entity}: {missing_types}"
        )

    query = _clean_records_query(
        dataset=dataset,
        columns=columns,
        types=types,
    )

    records = spark_io.read(
        query,
        predicates=spark_io.hash_predicates("record_id"),
    )

    if records.limit(1).count() == 0:
        raise RuntimeError(
            f"No accepted records found for entity '{entity}'."
        )

    # Majority State:
    # A customer may appear in many transaction rows in the same ingestion.
    # Count occurrences of each complete entity state, then select the
    # unique state with the highest frequency.
    #
    # Never select attributes independently, because that could construct
    # a combination of values that never existed in the source records.

    tracked_columns = [*attributes, *references]

    state_counts = (
        records
        .filter(F.col(key).isNotNull())
        .groupBy(
            key,
            "run_id",
            "ingested_at",
            *tracked_columns,
        )
        .agg(
            F.count("*").alias("_state_count")
        )
    )

    # Rank complete states by their frequency within each ingestion.
    state_window = (
        Window
        .partitionBy(key, "run_id", "ingested_at")
        .orderBy(F.col("_state_count").desc())
    )

    ranked_states = state_counts.withColumn(
        "_state_rank",
        F.dense_rank().over(state_window),
    )

    # If multiple states share rank 1, the majority is ambiguous.
    tied_states = (
        ranked_states
        .filter(F.col("_state_rank") == 1)
        .groupBy(key, "run_id", "ingested_at")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .collect()
    )

    if tied_states:
        example = tied_states[0].asDict()
        raise RuntimeError(
            f"Tied majority states for {entity} within one ingestion: {example}"
        )

    # Keep exactly one complete state: the unique most frequent state.
    snapshots = (
        ranked_states
        .filter(F.col("_state_rank") == 1)
        .drop("_state_count", "_state_rank")
    )

    # Reject duplicate observation timestamps for the same entity.
    # Otherwise two versions could have the same valid_from timestamp.
    duplicate_times = (
        snapshots
        .groupBy(key, "ingested_at")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .collect()
    )

    if duplicate_times:
        example = duplicate_times[0].asDict()
        raise RuntimeError(
            f"Duplicate observation timestamps for {entity}: {example}"
        )

    # Compare each observed state with the previous observed state.
    history_window = (
        Window
        .partitionBy(key)
        .orderBy(
            F.col("ingested_at").asc(),
            F.col("run_id").asc(),
        )
    )

    compared = snapshots.withColumn(
        "_version_number",
        F.row_number().over(history_window),
    )

    changed_condition = F.col("_version_number") == 1

    for column in tracked_columns:
        previous_value = F.lag(
            F.col(column)
        ).over(history_window)

        compared = compared.withColumn(
            f"_previous_{column}",
            previous_value,
        )

        changed_condition = (
            changed_condition
            | ~F.col(column).eqNullSafe(
                F.col(f"_previous_{column}")
            )
        )

    changed = compared.filter(changed_condition)

    # Calculate the end of each version.
    version_window = (
        Window
        .partitionBy(key)
        .orderBy(
            F.col("ingested_at").asc(),
            F.col("run_id").asc(),
        )
    )

    result = (
        changed
        .withColumn(
            "valid_to",
            F.lead("ingested_at").over(version_window),
        )
        .withColumnRenamed(
            "ingested_at",
            "valid_from",
        )
        .withColumn(
            "is_current",
            F.col("valid_to").isNull(),
        )
    )

    # Deterministic surrogate key for each entity version.
    surrogate_window = Window.orderBy(
        F.col(key).asc(),
        F.col("valid_from").asc(),
    )

    result = result.withColumn(
        f"{entity}_sk",
        F.row_number().over(surrogate_window).cast("long"),
    )

    # Remove helper columns used for change detection.
    helper_columns = [
        column
        for column in result.columns
        if column.startswith("_previous_")
        or column == "_version_number"
    ]

    result = result.drop(*helper_columns)

    output_columns = [
        f"{entity}_sk",
        key,
        *attributes,
        *references,
        "valid_from",
        "valid_to",
        "is_current",
    ]

    return result.select(*output_columns)
