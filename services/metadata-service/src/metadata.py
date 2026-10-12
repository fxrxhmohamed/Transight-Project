"""Metadata engine: describe silver for the AutoETL step (PySpark).

Combines what the schema registry says about every column (type, role,
entity, key, PII, description, unit) with what silver actually holds, and
stores it in the meta schema (sql/03_meta_gold.sql):

    meta.dataset        the silver table, rows, runs, grain and primary key
    meta.column         per column: types, role, entity, flags, nulls,
                        distinct values, min and max
    meta.entity         per entity: key, attributes, distinct keys
    meta.relationship   per reference between entities: keys without one

Silver keeps one row per record and run; the description is of the
current data: per primary key the row of the latest run. Min and max are
left out for personal data (pii: true).

Usage
-----
    python src/metadata.py
    python src/metadata.py --rules config/rules/quality_rules.yaml
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Shared code: database, Spark and the schema registry of the dq-service.
ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "services" / "common" / "src"))
sys.path.append(str(ROOT / "services" / "dq-service" / "src"))
import bronze  # noqa: E402
import schemas  # noqa: E402
import spark_io  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from registry import ConfigError, Registry, load_registry  # noqa: E402

DEFAULT_RULES = ROOT / "config" / "rules" / "quality_rules.yaml"

# Silver columns that are not data.
SYSTEM_COLUMNS = ("run_id", "record_id", "dq_warnings")


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def silver_columns(conn, table: str) -> dict[str, str]:
    """Column -> PostgreSQL type of silver.<table>, in order."""

    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'silver' AND table_name = %s ORDER BY ordinal_position",
        (table,),
    ).fetchall()
    if not rows:
        raise ConfigError(f"silver.{table} does not exist; run the merge first.")
    return dict(rows)


def current_records(table: str, types: dict[str, str], primary_key: list[str]) -> DataFrame:
    """Silver with one row per primary key: the latest run's."""

    select = ", ".join(
        f"{quote(c)}::text AS {quote(c)}" if types[c].startswith("time") else quote(c)
        for c in types
    )
    df = spark_io.read(f"SELECT {select} FROM silver.{quote(table)}",
                       predicates=spark_io.hash_predicates("record_id"))
    keys = [k for k in primary_key if k in types]
    if not keys:
        return df
    partition = ", ".join(ident(k) for k in keys)
    present = " AND ".join(f"{ident(k)} IS NOT NULL" for k in keys)
    return df.selectExpr(
        "*", f"row_number() OVER (PARTITION BY {partition} "
             "ORDER BY run_id DESC, record_id DESC) AS __latest",
    ).filter(f"__latest = 1 OR NOT ({present})").drop("__latest")


def generate(registry: Registry) -> dict[str, Any]:
    dataset = registry.dataset
    name = dataset.get("name", "dataset")
    table = bronze.table_name_of(name)
    primary_key = list(dataset.get("primary_key") or [])
    entities = registry.schema.get("entities") or {}

    with bronze.connect() as conn:
        schemas.ensure(conn, "02_dq.sql", "03_meta_gold.sql")
        types = silver_columns(conn, table)
    data_columns = [c for c in types if c not in SYSTEM_COLUMNS]

    df = current_records(table, types, primary_key).persist(StorageLevel.DISK_ONLY)

    # ---- One pass over the data ----
    aggregates = ["count(1) AS rows", "count(DISTINCT run_id) AS runs"]
    for j, c in enumerate(data_columns):
        aggregates += [
            f"sum(CASE WHEN {ident(c)} IS NULL THEN 1 ELSE 0 END) AS n{j}",
            f"approx_count_distinct({ident(c)}) AS d{j}",
            f"CAST(min({ident(c)}) AS STRING) AS lo{j}",
            f"CAST(max({ident(c)}) AS STRING) AS hi{j}",
        ]
    stats = df.selectExpr(*aggregates).collect()[0].asDict()

    # Exact key counts, one small job each (several exact distinct counts
    # in one aggregation would copy every row once per count).
    key_of = {e: spec["key"] for e, spec in entities.items() if spec["key"] in types}
    for e, key in key_of.items():
        keys = df.select(key).where(f"{ident(key)} IS NOT NULL")
        stats[f"k_{e}"] = keys.distinct().count()
        for target, column in (entities[e].get("references") or {}).items():
            if column in types:
                stats[f"r_{e}_{target}"] = (
                    df.where(f"{ident(key)} IS NOT NULL AND {ident(column)} IS NULL")
                    .select(key).distinct().count()
                )
    total = stats["rows"]
    with bronze.connect() as conn:
        all_rows = conn.execute(f"SELECT count(*) FROM silver.{quote(table)}").fetchone()[0]
    df.unpersist()

    grain_entity = next(
        (e for e, key in key_of.items() if [key] == primary_key), None,
    )

    columns = []
    for j, c in enumerate(data_columns):
        spec = registry.columns.get(c, {})
        entity = spec.get("entity")
        pii = bool(spec.get("pii"))
        columns.append({
            "column_name": c,
            "ordinal": j + 1,
            "logical_type": spec.get("type", "string"),
            "database_type": types[c],
            "role": spec.get("role"),
            "entity": entity,
            "is_key": any(c == key for key in key_of.values()),
            "is_pii": pii,
            "generated": bool(spec.get("generated")),
            "nullable": None if "nullable" not in spec else str(spec["nullable"]).lower(),
            "unit": spec.get("unit"),
            "description": spec.get("description"),
            "null_count": stats[f"n{j}"] or 0,
            "null_rate": round((stats[f"n{j}"] or 0) / total, 6) if total else 0,
            "distinct_count": stats[f"d{j}"] or 0,
            "min_value": None if pii else stats[f"lo{j}"],
            "max_value": None if pii else stats[f"hi{j}"],
        })

    entity_rows = [{
        "entity": e,
        "key_column": key,
        "attributes": [a for a in entities[e].get("attributes", []) if a in types],
        "is_grain": e == grain_entity,
        "description": entities[e].get("description"),
        "distinct_keys": stats[f"k_{e}"],
    } for e, key in key_of.items()]

    relationships = [{
        "from_entity": e,
        "from_column": column,
        "to_entity": target,
        "to_column": key_of[target],
        "null_references": stats[f"r_{e}_{target}"],
    } for e in key_of for target, column in (entities[e].get("references") or {}).items()
        if column in types and target in key_of]

    return {
        "dataset": {
            "dataset": name,
            "description": (dataset.get("description") or "").strip() or None,
            "silver_table": f"silver.{table}",
            "grain": dataset.get("grain"),
            "grain_entity": grain_entity,
            "primary_key": primary_key,
            "row_count": all_rows,
            "record_count": total,
            "run_count": stats["runs"],
            "schema_version": str(registry.schema.get("version")),
            "generated_at": datetime.now(timezone.utc),
        },
        "columns": columns,
        "entities": entity_rows,
        "relationships": relationships,
    }


def store(meta: dict[str, Any]) -> None:
    """Replace the dataset's metadata in one transaction."""

    name = meta["dataset"]["dataset"]

    def insert(conn, table: str, row: dict[str, Any]) -> None:
        conn.execute(
            f"INSERT INTO meta.{table} ({', '.join(quote(k) for k in row)}) "
            f"VALUES ({', '.join(['%s'] * len(row))})",
            list(row.values()),
        )

    with bronze.connect() as conn, conn.transaction():
        conn.execute("DELETE FROM meta.dataset WHERE dataset = %s", (name,))
        insert(conn, "dataset", meta["dataset"])
        for table, rows in (("column", meta["columns"]), ("entity", meta["entities"]),
                            ("relationship", meta["relationships"])):
            for row in rows:
                insert(conn, table, {"dataset": name, **row})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Describe silver in the meta schema.")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    meta = generate(registry)
    store(meta)

    dataset = meta["dataset"]
    print(f"{dataset['silver_table']}: {dataset['record_count']} current records "
          f"({dataset['row_count']} rows in {dataset['run_count']} runs), "
          f"grain {dataset['grain_entity']}")
    print(f"  meta.column        {len(meta['columns'])} columns "
          f"({sum(c['is_pii'] for c in meta['columns'])} personal data)")
    print("  meta.entity        " + ", ".join(
        f"{e['entity']} ({e['distinct_keys']})" for e in meta["entities"]))
    print("  meta.relationship  " + ", ".join(
        f"{r['from_entity']}->{r['to_entity']}" for r in meta["relationships"]))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ConfigError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    finally:
        spark_io.stop()
