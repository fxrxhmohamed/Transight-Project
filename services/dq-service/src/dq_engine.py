"""Data Quality Engine.

Reads the schema registry and quality rules, validates a batch,
remediates what the rules allow, and splits the records into:

    valid       clean, standardised records (warnings flagged per row)
    quarantine  records with an unresolved critical failure, with their
                cleaned values, and one error line per failed rule

Every record is identified by its record id (from its bronze
ingestion), so it can be followed through quarantine, correction and
re-validation (quarantine.py). Every automatic repair is written to the
remediation audit and a run summary to dq.round_result. Everything is
stored in PostgreSQL; see layout.py for the tables.

Usage
-----
    # An ingestion in the bronze layer (see services/ingestion-service)
    python src/dq_engine.py --ingestion ING_20261011_001
    python src/dq_engine.py --ingestion latest

    # Validate again from scratch (removes that run's outputs)
    python src/dq_engine.py --ingestion ING_20261011_001 --force

    # An ad hoc file without ingestion (record ids ROW_000001, ...)
    python src/dq_engine.py --input data/raw/banking/extract.csv
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from checks import (
    condition_mask,
    dependency_columns,
    detect,
    functional_dependency,
    generated_values,
    null_token_mask,
    null_tokens,
    whitespace_mask,
)
import db
from layout import (
    AUDIT_COLUMNS,
    ERROR_COLUMNS,
    OPEN,
    STATUS_COLUMNS,
    Layout,
    LayoutError,
    now_utc,
    record_ids,
    save_status,
)
from registry import ConfigError, Registry, Rule, load_registry
from remediation import remediate
from values import as_flags, canonical_text, parse_values

# The bronze layer client (db.py puts the ingestion service on the path).
bronze = db.bronze


# ============================================================
# Defaults
# ============================================================

def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_RULES = PROJECT_ROOT / "config" / "rules" / "quality_rules.yaml"


# ============================================================
# Batch state
# ============================================================

class Batch:
    """The records being validated, with their working (cleaned) values.

    raw     input text (never modified)
    data    working text, updated by remediation; generated columns that
            the input lacks are added empty, in schema order
    active  False for rows removed as exact duplicates
    focus   rows counted in the rule statistics (all by default; the
            corrected records when re-validating)
    """

    def __init__(
        self,
        raw: pd.DataFrame,
        registry: Registry,
        ids: pd.Series,
        focus: pd.Series | None = None,
    ) -> None:
        self.raw = raw
        self.data = raw.copy()
        self.registry = registry
        self.record_ids = pd.Series(ids.to_numpy(), index=raw.index, dtype="string")

        # Generated columns the input does not provide: filled by their
        # generate rule, without an audit entry per row.
        self.filled_columns = {
            column for column, spec in registry.columns.items()
            if spec.get("generated") and column not in raw.columns
        }
        if self.filled_columns:
            order = [c for c in registry.columns if c in raw.columns
                     or c in self.filled_columns]
            order += [c for c in raw.columns if c not in registry.columns]
            self.data = self.data.reindex(columns=order).astype("string")

        self.active = pd.Series(True, index=raw.index)
        self.focus = (
            pd.Series(True, index=raw.index) if focus is None
            else pd.Series(focus.to_numpy(), index=raw.index)
        )
        self.failures: list[tuple[Rule, pd.Series]] = []
        # Per rule: the text shown as an error's value, when the failing
        # column's value says too little (e.g. what was changed).
        self.error_values: dict[str, pd.Series] = {}
        self.audit: list[pd.DataFrame] = []
        self._typed: dict[str, pd.Series] = {}

    def spec(self, column: str) -> dict[str, Any]:
        return self.registry.columns.get(column, {"type": "string"})

    def typed(self, column: str) -> pd.Series:
        """Typed view of a column (cached until the column changes)."""

        if column not in self._typed:
            self._typed[column] = parse_values(
                self.data[column], self.spec(column),
            )
        return self._typed[column]

    def count(self, mask: pd.Series) -> int:
        """Rows of a mask that count in the rule statistics."""

        return int((mask & self.focus).sum())

    def write(self, column: str, new_values: pd.Series) -> None:
        self.data.loc[new_values.index, column] = new_values
        self._typed.pop(column, None)

    def record_audit(
        self,
        rule: Rule,
        column: str,
        index: pd.Index,
        original: pd.Series,
        action: str,
        outcome: pd.Series | str,
    ) -> None:
        if not len(index):
            return

        self.audit.append(pd.DataFrame({
            "record_id": self.record_ids[index].to_numpy(),
            "column": column,
            "original_value": original[index].to_numpy(),
            "cleaned_value": self.data.loc[index, column].to_numpy()
            if column in self.data.columns else pd.NA,
            "rule_id": rule.id,
            "action": action,
            "outcome": outcome.to_numpy()
            if isinstance(outcome, pd.Series) else outcome,
        }))

    def fail(self, rule: Rule, failing: pd.Series) -> None:
        if failing.any():
            self.failures.append((rule, failing))


# ============================================================
# Rule execution
# ============================================================

def new_stats(rule: Rule) -> dict[str, Any]:
    return {
        **rule.describe(),
        "checked": 0,
        "detected": 0,
        "remediated": 0,
        "unresolved": 0,
    }


def run_value_rule(
    batch: Batch,
    rule: Rule,
    stats: dict[str, Any],
    column: str | None = None,
) -> None:
    """Detect -> remediate -> re-check for one rule on one column."""

    column = column or rule.column
    scope = batch.active & condition_mask(batch, rule.when)

    if rule.type == "trim_whitespace":
        failing = scope & whitespace_mask(batch.data[column])
    elif rule.type == "null_tokens":
        failing = scope & null_token_mask(batch.data[column], null_tokens(batch))
    else:
        failing = detect(batch, rule, scope)

    stats["checked"] += batch.count(scope)
    stats["detected"] += batch.count(failing)

    if rule.remediation and failing.any():
        # Usually the rule's own column; derived rules may repair their
        # source column instead.
        target = rule.remediation.get("target", column)
        original = batch.data[target].copy()
        bound = Rule(**{**rule.__dict__, "column": column})
        new_values, changed = remediate(batch, bound, failing)
        batch.write(target, new_values[changed])

        if rule.type in ("trim_whitespace", "null_tokens"):
            still = failing & ~failing.index.isin(changed[changed].index)
        else:
            still = detect(batch, rule, scope)

        attempted = failing[failing].index
        outcome = pd.Series("remediated", index=attempted)
        outcome[still[attempted]] = "not_fixed"
        batch.record_audit(
            rule, target, attempted, original,
            rule.remediation["action"], outcome,
        )
        failing = still

    stats["unresolved"] += batch.count(failing)
    stats["remediated"] = stats["detected"] - stats["unresolved"]

    batch.fail(rule, failing)


def run_duplicate_rows(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    """Exact copies of an earlier row (on the input text)."""

    duplicated = batch.active & batch.raw.duplicated(keep="first")
    stats["checked"] = batch.count(batch.active)
    stats["detected"] = batch.count(duplicated)

    if (rule.remediation or {}).get("action") == "drop_duplicates":
        index = duplicated[duplicated].index
        batch.active &= ~duplicated
        empty = pd.Series(pd.NA, index=batch.raw.index, dtype="string")
        batch.record_audit(
            rule, "*", index, empty, "drop_duplicates", "removed",
        )
        stats["remediated"] = stats["detected"]
        return

    stats["unresolved"] = stats["detected"]
    batch.fail(rule, duplicated)


def run_dependency(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    scope = batch.active & condition_mask(batch, rule.when)
    failing, per_column = functional_dependency(batch, rule, scope)

    stats["checked"] = batch.count(scope)
    stats["detected"] = stats["unresolved"] = batch.count(failing)
    stats["conflicts_by_column"] = {
        column: count for column, count in per_column.items() if count
    }
    batch.fail(rule, failing)


def run_generate(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    """Fill a generated column from cleaned columns.

    When the input lacks the column it is simply filled. When the input
    has it, values that differ from the computed ones are replaced and
    audited. Rows whose value cannot be computed fail the rule.
    """

    column = rule.column
    scope = batch.active & condition_mask(batch, rule.when)
    expected = generated_values(batch, rule)
    missing = scope & expected.isna()

    stats["checked"] = batch.count(scope)

    if column in batch.filled_columns:
        batch.write(column, expected[scope])
    else:
        current = batch.data[column]
        differs = scope & expected.notna() & ~as_flags(current.eq(expected))
        if differs.any():
            original = current.copy()
            batch.write(column, expected[differs])
            batch.record_audit(
                rule, column, differs[differs].index, original,
                "derive", "remediated",
            )
        stats["remediated"] = batch.count(differs)

    stats["unresolved"] = batch.count(missing)
    stats["detected"] = stats["remediated"] + stats["unresolved"]
    batch.fail(rule, missing)


def run_auto_fix_review(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    """Records with many automatic repairs: a person has to confirm them.

    Counts the repairs made so far (so it runs after every other rule);
    min_changes (default 3) or more fail the rule. ignore_actions lists
    remediation actions that do not count (e.g. trim).
    """

    minimum = int(rule.params.get("min_changes", 3))
    ignore = set(rule.params.get("ignore_actions") or [])
    scope = batch.active
    stats["checked"] = batch.count(scope)

    if not batch.audit:
        return

    audit = pd.concat(batch.audit, ignore_index=True)
    edits = audit[audit["outcome"].eq("remediated") & ~audit["action"].isin(ignore)]
    per_record = edits.groupby("record_id").size()
    counts = batch.record_ids.map(per_record).fillna(0).astype(int)
    failing = scope & counts.ge(minimum)

    stats["detected"] = stats["unresolved"] = batch.count(failing)
    if not failing.any():
        return

    # e.g. "3 values: transaction_time '07:19:20 PM' -> '19:19:20'; ..."
    failing_ids = set(batch.record_ids[failing])
    listed = edits[edits["record_id"].isin(failing_ids)]
    described = (
        listed["column"].astype(str) + " '"
        + listed["original_value"].fillna("").astype(str) + "' -> '"
        + listed["cleaned_value"].fillna("").astype(str) + "'"
    ).groupby(listed["record_id"]).agg("; ".join)
    batch.error_values[rule.id] = (
        counts.astype(str) + " values: " + batch.record_ids.map(described).fillna("")
    )
    batch.fail(rule, failing)


def run_rule(batch: Batch, rule: Rule) -> dict[str, Any]:
    stats = new_stats(rule)

    if rule.type == "auto_fix_review":
        run_auto_fix_review(batch, rule, stats)

    elif rule.type == "generate":
        run_generate(batch, rule, stats)

    elif rule.type == "duplicate_rows":
        run_duplicate_rows(batch, rule, stats)

    elif rule.type == "functional_dependency":
        run_dependency(batch, rule, stats)

    elif rule.type in ("trim_whitespace", "null_tokens"):
        columns = (
            list(batch.data.columns) if rule.columns in ([], ["*"])
            else rule.columns
        )
        for column in columns:
            run_value_rule(batch, rule, stats, column)

    else:
        run_value_rule(batch, rule, stats)

    return stats


# ============================================================
# Error list (why each record failed)
# ============================================================

class _Placeholders(dict):
    """format_map helper: unknown placeholders stay as written."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _bounds(params: dict[str, Any]) -> str:
    low, high = params.get("min"), params.get("max")
    return (
        ("(" if params.get("exclusive_min") else "[")
        + ("-inf" if low is None else str(low)) + " .. "
        + ("+inf" if high is None else str(high))
        + (")" if params.get("exclusive_max") else "]")
    )


def error_column(batch: Batch, rule: Rule) -> str | None:
    """Column an error points at (the one to correct)."""

    if rule.type == "functional_dependency":
        return dependency_columns(batch, rule)[0]
    if rule.type == "compare":
        return rule.params["left"]
    if rule.column:
        return rule.column
    if rule.type == "expression" and rule.columns:
        return rule.columns[0]
    return None


def build_errors(batch: Batch) -> pd.DataFrame:
    """One row per failed rule and record, with code and message."""

    parts: list[pd.DataFrame] = []

    for rule, mask in batch.failures:
        index = mask[mask].index
        column = error_column(batch, rule)
        values = (
            batch.error_values[rule.id][index] if rule.id in batch.error_values
            else batch.data.loc[index, column] if column in batch.data.columns
            else pd.Series(pd.NA, index=index, dtype="string")
        )

        context = _Placeholders(
            column=column or "",
            column_type=batch.spec(column).get("type", "string") if column else "",
            bounds=_bounds(rule.params),
            left=rule.params.get("left", ""),
            right=rule.params.get("right", ""),
            operator=rule.params.get("operator", ""),
            source=rule.params.get("source", ""),
            reference=(rule.params.get("reference") or {}).get("path", ""),
            determinant=column or "",
            name=rule.name,
        )
        template = rule.message_template

        if "{value}" in template:
            messages = [
                template.format_map(
                    _Placeholders(context, value="" if pd.isna(v) else v)
                )
                for v in values
            ]
        else:
            messages = template.format_map(context)

        parts.append(pd.DataFrame({
            "record_id": batch.record_ids[index].to_numpy(),
            "rule_id": rule.id,
            "rule_name": rule.name,
            "error_code": rule.code,
            "error_category": rule.error_category,
            "dimension": rule.dimension,
            "severity": rule.severity,
            "column": column,
            "value": values.to_numpy(),
            "message": messages,
        }))

    if not parts:
        return pd.DataFrame(columns=ERROR_COLUMNS)

    errors = pd.concat(parts, ignore_index=True)[ERROR_COLUMNS]
    return errors.sort_values("record_id", kind="stable", ignore_index=True)


# ============================================================
# Accept / quarantine
# ============================================================

def rule_id_lists(
    failures: list[tuple[Rule, pd.Series]],
    index: pd.Index,
) -> pd.Series:
    """';'-joined rule ids per row."""

    joined = pd.Series("", index=index, dtype=object)

    for rule, mask in failures:
        joined = joined.where(~mask, joined + rule.id + ";")

    return joined.str.rstrip(";")


@dataclass
class Validation:
    """Outcome of validating a batch."""

    batch: Batch
    accepted: pd.Series       # mask
    quarantined: pd.Series    # mask
    flagged: pd.Series        # mask: accepted with warnings
    valid: pd.DataFrame       # record_id, values, dq_warnings
    quarantine: pd.DataFrame  # record_id, values, dq_errors, dq_warnings
    errors: pd.DataFrame      # ERROR_COLUMNS
    audit: pd.DataFrame       # AUDIT_COLUMNS
    rule_stats: list[dict[str, Any]]


def split_records(batch: Batch) -> dict[str, Any]:
    """Decide accepted vs quarantined rows and build both outputs."""

    actions = batch.registry.severity_actions
    index = batch.data.index

    quarantine_failures = [
        (rule, mask) for rule, mask in batch.failures
        if actions[rule.severity] == "quarantine"
    ]
    flag_failures = [
        (rule, mask) for rule, mask in batch.failures
        if actions[rule.severity] == "accept_with_flag"
    ]

    quarantined = pd.Series(False, index=index)
    for _, mask in quarantine_failures:
        quarantined |= mask
    quarantined &= batch.active
    accepted = batch.active & ~quarantined

    # Values still invalid in accepted rows are nulled so the accepted
    # data always loads with its declared types.
    for rule, mask in batch.failures:
        if rule.on_accept != "set_null" or not rule.column:
            continue
        target = mask & accepted & batch.data[rule.column].notna()
        if target.any():
            original = batch.data[rule.column].copy()
            batch.write(
                rule.column,
                pd.Series(pd.NA, index=target[target].index, dtype="string"),
            )
            batch.record_audit(
                rule, rule.column, target[target].index, original,
                "set_null", "nullified_in_accepted",
            )

    warnings_text = rule_id_lists(flag_failures, index)
    errors_text = rule_id_lists(quarantine_failures, index)
    formats = batch.registry.dataset.get("canonical_formats", {})

    # Canonical text for dates and times; values that cannot be read keep
    # their working text so quarantined records show what is wrong.
    clean = batch.data.copy()
    for column in clean.columns:
        if column not in batch.registry.columns:
            continue
        text = canonical_text(batch.typed(column), batch.spec(column), formats)
        if text is not None:
            clean[column] = text.where(text.notna(), batch.data[column])

    clean.insert(0, "record_id", batch.record_ids)

    valid = clean[accepted].copy()
    valid["dq_warnings"] = warnings_text[accepted]

    quarantine = clean[quarantined].copy()
    quarantine["dq_errors"] = errors_text[quarantined]
    quarantine["dq_warnings"] = warnings_text[quarantined]

    return {
        "valid": valid,
        "quarantine": quarantine,
        "accepted": accepted,
        "quarantined": quarantined,
        "flagged": accepted & warnings_text.ne(""),
    }


def validate_frame(
    frame: pd.DataFrame,
    registry: Registry,
    ids: pd.Series,
    log: Callable[[str], None] = print,
    focus: pd.Series | None = None,
) -> Validation:
    """Run every rule on a batch and split it into valid and quarantine.

    focus limits the rule statistics to some rows (the others are context
    for cross-row rules).
    """

    batch = Batch(frame.reset_index(drop=True), registry, ids, focus)
    rule_stats = []

    # Reviews of automatic repairs count the repairs, so they come last.
    rules = registry.all_rules()
    rules = (
        [r for r in rules if r.type != "auto_fix_review"]
        + [r for r in rules if r.type == "auto_fix_review"]
    )

    for rule in rules:
        step = time.perf_counter()
        stats = run_rule(batch, rule)
        rule_stats.append(stats)
        if stats["detected"]:
            log(f"  {rule.id:<45} detected {stats['detected']:>8}  "
                f"unresolved {stats['unresolved']:>8}  "
                f"({time.perf_counter() - step:.1f}s)")

    # Before the split: it nulls invalid values in accepted rows.
    errors = build_errors(batch)
    split = split_records(batch)

    audit = (
        pd.concat(batch.audit, ignore_index=True)
        if batch.audit else pd.DataFrame(columns=AUDIT_COLUMNS)
    )

    return Validation(
        batch=batch,
        accepted=split["accepted"],
        quarantined=split["quarantined"],
        flagged=split["flagged"],
        valid=split["valid"],
        quarantine=split["quarantine"],
        errors=errors,
        audit=audit,
        rule_stats=rule_stats,
    )


# ============================================================
# Results
# ============================================================

def summarize(
    result: Validation,
    run: dict[str, Any],
) -> dict[str, Any]:
    batch = result.batch
    input_rows = len(batch.raw)
    processed = int(batch.active.sum())
    accepted = int(result.accepted.sum())
    quarantined = int(result.quarantined.sum())

    def rate(part: int, whole: int) -> float:
        return round(part / whole * 100, 4) if whole else 0.0

    dimensions: dict[str, dict[str, int]] = {}
    for stats in result.rule_stats:
        bucket = dimensions.setdefault(stats["dimension"], {
            "rules": 0, "detected": 0, "remediated": 0, "unresolved": 0,
        })
        bucket["rules"] += 1
        for key in ("detected", "remediated", "unresolved"):
            bucket[key] += stats[key]

    columns = {
        column: {
            "missing_in_input": int(batch.raw[column].isna().sum()),
            "missing_in_accepted": int(
                batch.data.loc[result.accepted, column].isna().sum()
            ),
        }
        for column in batch.raw.columns
    }

    audit = result.audit
    outcomes = (
        audit.groupby(["action", "outcome"]).size()
        .rename("count").reset_index().to_dict("records")
        if len(audit) else []
    )

    in_quarantine = result.errors["record_id"].isin(
        batch.record_ids[result.quarantined]
    )
    errors_by_category = (
        result.errors[in_quarantine]
        .groupby(["error_category", "error_code"]).size()
        .rename("count").reset_index().to_dict("records")
    )

    return {
        **run,
        "counts": {
            "input_rows": input_rows,
            "duplicate_rows_removed": input_rows - processed,
            "processed_rows": processed,
            "accepted_rows": accepted,
            "accepted_with_warnings_rows": int(result.flagged.sum()),
            "quarantined_rows": quarantined,
            "acceptance_rate": rate(accepted, processed),
            "quarantine_rate": rate(quarantined, processed),
            "remediated_values": int(
                (audit["outcome"] == "remediated").sum()
            ) if len(audit) else 0,
        },
        "dimensions": dimensions,
        "quarantine_errors": errors_by_category,
        "remediation_outcomes": outcomes,
        "rules": result.rule_stats,
        "columns": columns,
    }


# ============================================================
# Main
# ============================================================

def read_source(path: Path, encoding: str, delimiter: str) -> pd.DataFrame:
    """Read every column as text; empty cells become null."""

    frame = pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
        sep=delimiter,
        encoding=encoding,
    )
    return frame.mask(frame.eq(""))


def check_columns(frame: pd.DataFrame, registry: Registry) -> dict[str, list]:
    """Schema drift: missing columns stop the run, extra ones are kept.

    Generated columns may be missing; the engine fills them.
    """

    expected = [
        column for column, spec in registry.columns.items()
        if not spec.get("generated")
    ]
    missing = [c for c in expected if c not in frame.columns]
    extra = [c for c in frame.columns if c not in registry.columns]

    if missing:
        raise ConfigError(f"Input is missing schema columns: {missing}")

    return {"missing_columns": missing, "extra_columns": extra}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a batch against the schema registry and "
        "quality rules; split valid and quarantined records.",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--ingestion", default="latest",
        help="Ingestion id in the bronze layer, or 'latest' (default).",
    )
    source.add_argument(
        "--input", type=Path, default=None,
        help="Ad hoc CSV file instead of an ingestion.",
    )
    parser.add_argument(
        "--rules", type=Path, default=DEFAULT_RULES,
        help="Quality rules YAML.",
    )
    parser.add_argument(
        "--schema", type=Path, default=None,
        help="Schema registry YAML (default: the one named in the rules).",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Run id for --input (default: UTC timestamp).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Validate a run again, removing its previous outputs "
        "(including quarantine rounds and corrections).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    clock = time.perf_counter()

    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset
    name = dataset.get("name", "dataset")
    source_cfg = dataset.get("source", {})

    if args.input:
        source = str(args.input)
        run_id = args.run_id or started.strftime("%Y%m%dT%H%M%SZ")
        encoding = source_cfg.get("encoding", "utf-8-sig")
        delimiter = source_cfg.get("delimiter", ",")
        prefix, width = "ROW_", 6
        manifest = None
    else:
        conn = bronze.connect()
        manifest = bronze.find_ingestion(conn, name, args.ingestion)
        source = f"bronze.{manifest['table_name']} ({manifest['source_file']})"
        run_id = manifest["ingestion_id"]
        prefix = manifest["record_id"]["prefix"]
        width = manifest["record_id"]["width"]

    layout = Layout(name, run_id)

    if layout.exists() and not args.force:
        raise ConfigError(
            f"Run {run_id} is already validated; use --force to start "
            "over (this removes its quarantine rounds and corrections)."
        )

    print(f"Run {run_id}: {source}")
    print(f"Loaded {len(registry.all_rules())} rules.")

    if manifest:
        with conn:
            frame = bronze.read_frame(conn, manifest)
        raw = frame.mask(frame.eq(""))
    else:
        raw = read_source(args.input, encoding, delimiter)
    drift = check_columns(raw, registry)
    if drift["extra_columns"]:
        print(f"Columns not in the schema (kept, not validated): "
              f"{drift['extra_columns']}")

    ids = record_ids(prefix, width, len(raw))
    result = validate_frame(raw, registry, ids)

    # ---- Outputs ----
    quarantined_ids = result.quarantine["record_id"]

    run = {
        "run_id": run_id,
        "round": 1,
        "dataset": name,
        "source_file": source,
        "ingestion": manifest and {
            "ingestion_id": manifest["ingestion_id"],
            "sha256": manifest["sha256"],
            "ingested_at": manifest["ingested_at"],
        },
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - clock, 2),
        "config": {
            "rules_file": str(args.rules),
            "schema_version": registry.schema.get("version"),
            "rules_version": registry.rules_config.get("version"),
            "rule_count": len(registry.all_rules()),
        },
        "schema_drift": drift,
        "outputs": {
            "valid": ".".join(db.record_table_name(name, "valid")),
            "quarantine_records": ".".join(db.record_table_name(name, "quarantine")),
            "quarantine_errors": "dq.quarantine_error",
            "quarantine_status": "dq.quarantine_status",
            "audit": "dq.remediation_audit",
            "result": "dq.round_result",
        },
    }
    summary = summarize(result, run)

    # One transaction: a run is stored completely or not at all.
    with db.transaction():
        layout.delete()
        layout.start(
            [c for c in result.valid.columns if c not in ("record_id", "dq_warnings")],
            source,
        )
        layout.save_valid(result.valid)
        layout.save_quarantine(
            1, result.quarantine,
            result.errors[result.errors["record_id"].isin(quarantined_ids)],
        )
        layout.save_audit(1, result.audit)
        save_status(layout, pd.DataFrame({
            "record_id": quarantined_ids.to_numpy(),
            "status": OPEN,
            "round": 1,
            "attempts": 0,
            "updated_at": now_utc(),
        }, columns=STATUS_COLUMNS))
        layout.save_result(1, summary)

    counts = summary["counts"]
    print(
        f"\nValid {counts['accepted_rows']} "
        f"({counts['accepted_with_warnings_rows']} with warnings), "
        f"quarantined {counts['quarantined_rows']}, "
        f"duplicates removed {counts['duplicate_rows_removed']}."
    )
    for key, table in summary["outputs"].items():
        print(f"  {key:<19} {table}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError, bronze.BronzeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
