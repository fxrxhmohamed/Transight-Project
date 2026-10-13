"""AutoETL: build the gold star schema from the metadata (PySpark).

Reads meta.dataset / meta.column / meta.entity (written by the metadata
engine) and silver, and generates one table per entity, with no code per
dataset:

    gold.fact_<grain entity>   one row per record of the grain (latest run):
                               its own columns, the key of every other
                               entity on the row, and run_id / record_id
                               for lineage back to silver and bronze
    gold.dim_<entity>          one row per key of every other entity: its
                               attributes and the keys it references, taken
                               from the key's most recent record
    gold.dim_date              one row per day between the first and last
                               date in the fact table

Column types are silver's. Every table is built under a temporary name
and swapped in, all in one transaction, so readers (Power BI) never see a
half-built star.

Usage
-----
    python src/gold.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "services" / "common" / "src"))
sys.path.append(str(ROOT / "services" / "dq-service" / "src"))
sys.path.append(str(ROOT / "services" / "metadata-service" / "src"))
import bronze  # noqa: E402
import schemas  # noqa: E402
import spark_io  # noqa: E402
from metadata import current_records, ident, quote, silver_columns  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from registry import ConfigError, load_registry  # noqa: E402

DEFAULT_RULES = ROOT / "config" / "rules" / "quality_rules.yaml"

DATE_COLUMNS = {
    "date_key": "date",
    "year": "integer",
    "quarter": "integer",
    "month": "integer",
    "month_name": "text",
    "year_month": "text",
    "day": "integer",
    "day_of_week": "integer",      # 1 = Monday
    "day_name": "text",
    "week_of_year": "integer",
    "is_weekend": "boolean",
}


def load_meta(dataset: str) -> dict[str, Any]:
    with bronze.connect() as conn:
        schemas.ensure(conn, "02_dq.sql", "03_meta_gold.sql")
        row = conn.execute(
            "SELECT silver_table, grain_entity, primary_key FROM meta.dataset "
            "WHERE dataset = %s", (dataset,),
        ).fetchone()
        if row is None:
            raise ConfigError(f"No metadata for {dataset}; run the metadata step first.")
        columns = conn.execute(
            "SELECT column_name, entity, is_key FROM meta.column "
            "WHERE dataset = %s ORDER BY ordinal", (dataset,),
        ).fetchall()
        entities = conn.execute(
            "SELECT entity, key_column, attributes, is_grain FROM meta.entity "
            "WHERE dataset = %s ORDER BY entity", (dataset,),
        ).fetchall()
        references = conn.execute(
            "SELECT from_entity, from_column FROM meta.relationship WHERE dataset = %s",
            (dataset,),
        ).fetchall()
    return {
        "silver_table": row[0], "grain": row[1], "primary_key": list(row[2] or []),
        "columns": columns, "entities": entities, "references": references,
    }


def plan(meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Table name -> key and columns of every gold table."""

    if not meta["grain"]:
        raise ConfigError("The dataset has no grain entity (an entity keyed by the "
                          "primary key); gold needs one for its fact table.")
    keys = {e: key for e, key, _, _ in meta["entities"]}
    referenced = {}
    for entity, column in meta["references"]:
        referenced.setdefault(entity, []).append(column)

    tables = {}
    grain_key = keys[meta["grain"]]
    fact_columns = [grain_key] + [
        c for c, entity, is_key in meta["columns"]
        if entity == meta["grain"] and c != grain_key
    ]
    fact_columns += [key for e, key in keys.items()
                     if e != meta["grain"] and key not in fact_columns]
    tables[f"fact_{bronze.table_name_of(meta['grain'])}"] = {
        "key": [grain_key], "columns": [*fact_columns, "run_id", "record_id"],
    }

    for entity, key, attributes, is_grain in meta["entities"]:
        if is_grain:
            continue
        columns = [key] + [a for a in attributes if a != key]
        columns += [c for c in referenced.get(entity, []) if c not in columns]
        tables[f"dim_{bronze.table_name_of(entity)}"] = {"key": [key], "columns": columns}
    return tables


def written(df: DataFrame, columns: list[str], types: dict[str, str]) -> DataFrame:
    """Dates as ISO text (PostgreSQL converts them; no time zone shifts)."""

    return df.selectExpr(*[
        f"date_format({ident(c)}, 'yyyy-MM-dd') AS {ident(c)}" if types[c] == "date"
        else ident(c)
        for c in columns
    ])


def dim_date(fact: DataFrame, date_columns: list[str]) -> DataFrame | None:
    if not date_columns:
        return None
    lows = [f"min({ident(c)})" for c in date_columns]
    highs = [f"max({ident(c)})" for c in date_columns]
    bounds = fact.selectExpr(
        (f"least({', '.join(lows)})" if len(lows) > 1 else lows[0]) + " AS lo",
        (f"greatest({', '.join(highs)})" if len(highs) > 1 else highs[0]) + " AS hi",
    ).collect()[0]
    if bounds["lo"] is None:
        return None
    return spark_io.session().sql(
        f"SELECT explode(sequence(DATE '{bounds['lo']}', DATE '{bounds['hi']}', "
        "INTERVAL 1 DAY)) AS d"
    ).selectExpr(
        "date_format(d, 'yyyy-MM-dd') AS date_key",
        "year(d) AS year", "quarter(d) AS quarter", "month(d) AS month",
        "date_format(d, 'MMMM') AS month_name", "date_format(d, 'yyyy-MM') AS year_month",
        "day(d) AS day", "weekday(d) + 1 AS day_of_week", "date_format(d, 'EEEE') AS day_name",
        "weekofyear(d) AS week_of_year", "weekday(d) >= 5 AS is_weekend",
    )


def build(dataset: str) -> dict[str, int]:
    meta = load_meta(dataset)
    table = meta["silver_table"].split(".", 1)[1]
    with bronze.connect() as conn:
        types = silver_columns(conn, table)
    records = current_records(table, types, meta["primary_key"]).persist(
        StorageLevel.MEMORY_AND_DISK)

    tables = plan(meta)
    frames: dict[str, DataFrame] = {}
    ddl: dict[str, tuple[list[str], dict[str, str]]] = {}
    for name, spec in tables.items():
        key, columns = spec["key"], spec["columns"]
        if name.startswith("fact_"):
            df = records.filter(" AND ".join(f"{ident(k)} IS NOT NULL" for k in key))
        else:
            # The key's most recent record.
            df = records.filter(f"{ident(key[0])} IS NOT NULL").selectExpr(
                "*", f"row_number() OVER (PARTITION BY {ident(key[0])} "
                     "ORDER BY run_id DESC, record_id DESC) AS __latest",
            ).filter("__latest = 1")
        frames[name] = written(df, columns, types)
        ddl[name] = (key, {c: types[c] for c in columns})

    fact_name = next(n for n in tables if n.startswith("fact_"))
    dates = [c for c in tables[fact_name]["columns"] if types.get(c) == "date"]
    calendar = dim_date(records, dates)
    if calendar is not None:
        frames["dim_date"] = calendar
        ddl["dim_date"] = (["date_key"], DATE_COLUMNS)

    # ---- Load under temporary names, then swap in one transaction ----
    with bronze.connect() as conn:
        for name, (key, columns) in ddl.items():
            staged = quote(f"{name}__new")
            conn.execute(f"DROP TABLE IF EXISTS gold.{staged}")
            conn.execute(
                f"CREATE TABLE gold.{staged} ("
                + ", ".join(f"{quote(c)} {kind}" for c, kind in columns.items())
                + f", PRIMARY KEY ({', '.join(quote(k) for k in key)}))"
            )
        conn.commit()

    counts = {}
    for name, df in frames.items():
        spark_io.write(df, f"gold.{quote(name + '__new')}")
        counts[name] = df.count()
    records.unpersist()

    # CASCADE: anything still depending on the old tables goes with them.
    with bronze.connect() as conn, conn.transaction():
        for name in ddl:
            conn.execute(f"DROP TABLE IF EXISTS gold.{quote(name)} CASCADE")
            conn.execute(f"ALTER TABLE gold.{quote(name + '__new')} RENAME TO {quote(name)}")
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the gold star schema from the metadata.")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    counts = build(registry.dataset.get("name", "dataset"))
    for name, rows in counts.items():
        print(f"  gold.{name:<24} {rows:>9} rows")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ConfigError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    finally:
        spark_io.stop()
