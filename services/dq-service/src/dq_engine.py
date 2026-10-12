"""Data Quality Engine (PySpark).

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

How rules run on Spark
----------------------
Rules apply in order and later rules see the values earlier ones
repaired. Each rule becomes one step (trim and null-token rules one step
per column); consecutive steps that do not read or write a column another
of them writes run together in one projection (a stage), so the plan
stays shallow while the result is the same as applying them one by one.
Cross-row rules use window functions (unique values, one value per key);
the review of automatic repairs gets a stage of its own. Each step leaves per row

    __c<n>   0 not checked, 1 passed, 2 failed and repaired, 3 failed
    __a<n>   its audit entry (column, before, after, rule, action, outcome)

from which the rule statistics, the error list and the audit are built.
Expressions are generated as Spark SQL text and sent one projection at a
time (selectExpr), which keeps planning fast; no Python UDFs are used, so
every expression runs in the JVM.

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
import dataclasses
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import db
from checks import (
    condition,
    condition_columns,
    dependency_columns,
    detect,
    generated_values,
    null_token_failures,
    null_tokens,
    whitespace_failures,
)
from layout import Layout, LayoutError, now_utc
from registry import ConfigError, Registry, Rule, load_registry
from remediation import remediate
from values import (
    NULL,
    all_of,
    any_of,
    canonical_text,
    col,
    flag,
    ident,
    lit,
    parse_values,
)

bronze = db.bronze
spark_io = db.spark_io


# ============================================================
# Defaults
# ============================================================

def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_RULES = PROJECT_ROOT / "config" / "rules" / "quality_rules.yaml"

# Engine columns (data columns never start with "__").
RID, ROW, ACTIVE, FOCUS, DUP = "__rid", "__row", "__active", "__focus", "__dup"
ACCEPTED, QUARANTINED = "__accepted", "__quarantined"
WARNINGS, ERRORS = "__warnings", "__errors"

# Lineage is cut every this many stages (keeps planning time flat).
CHECKPOINT_EVERY = 6

AUDIT_FIELDS = ["column", "original_value", "cleaned_value", "rule_id", "action", "outcome"]


def _audit(column: str, original: str, cleaned: str, rule_id: str,
           action: str, outcome: str) -> str:
    """An audit entry as a SQL struct (every field text)."""

    return (
        f"named_struct('column', CAST({column} AS STRING), "
        f"'original_value', CAST({original} AS STRING), "
        f"'cleaned_value', CAST({cleaned} AS STRING), "
        f"'rule_id', CAST({rule_id} AS STRING), 'action', {lit(action)}, "
        f"'outcome', CAST({outcome} AS STRING))"
    )


def _code(scope: str, failing: str, still: str | None = None) -> str:
    """0 not checked, 1 passed, 2 failed and repaired, 3 failed."""

    if still is None:
        body = f"WHEN {failing} THEN 3 WHEN {scope} THEN 1"
    else:
        body = f"WHEN {still} THEN 3 WHEN {failing} THEN 2 WHEN {scope} THEN 1"
    return f"CAST(CASE {body} ELSE 0 END AS TINYINT)"


def _as(sql: str, name: str) -> str:
    return f"{sql} AS {ident(name)}"


# ============================================================
# Steps
# ============================================================

@dataclass
class Step:
    """One rule applied to one column (or to the whole row)."""

    index: int
    rule: Rule
    column: str | None = None
    reads: set[str] = field(default_factory=set)
    writes: set[str] = field(default_factory=set)
    solo: bool = False
    audit_column: str | None = None   # column named in its audit entries
    audit_action: str | None = None   # None when it never audits
    has_value: bool = False           # error value comes from __v<n>
    fd_columns: list[str] = field(default_factory=list)

    def name(self, kind: str) -> str:
        """__<kind><n>: c code, a audit, v error value; s, f, r, h, e
        temporaries (scope, failing, repaired, changed, expected)."""

        return f"__{kind}{self.index}"

    @property
    def code(self) -> str:
        return self.name("c")

    @property
    def audit(self) -> str:
        return self.name("a")

    @property
    def value(self) -> str:
        return self.name("v")


def build_steps(registry: Registry, data_columns: list[str], filled: set[str]) -> list[Step]:
    # Reviews of automatic repairs count the repairs, so they come last.
    rules = registry.all_rules()
    rules = (
        [r for r in rules if r.type != "auto_fix_review"]
        + [r for r in rules if r.type == "auto_fix_review"]
    )

    steps: list[Step] = []

    def add(rule: Rule, **kwargs: Any) -> Step:
        step = Step(index=len(steps), rule=rule, **kwargs)
        steps.append(step)
        return step

    for rule in rules:
        when = condition_columns(rule.when)

        if rule.type in ("trim_whitespace", "null_tokens"):
            columns = data_columns if rule.columns in ([], ["*"]) else rule.columns
            for column in columns:
                add(rule, column=column, reads={column, ACTIVE} | when,
                    writes={column} if rule.remediation else set(),
                    audit_column=column,
                    audit_action=(rule.remediation or {}).get("action"))

        elif rule.type == "duplicate_rows":
            dropping = (rule.remediation or {}).get("action") == "drop_duplicates"
            add(rule, reads={ACTIVE, DUP}, writes={ACTIVE} if dropping else set(),
                audit_column="*", audit_action="drop_duplicates" if dropping else None)

        elif rule.type == "functional_dependency":
            determinant, dependents = dependency_columns(registry, rule)
            add(rule, fd_columns=dependents,
                reads={determinant, *dependents, ACTIVE} | when)

        elif rule.type == "auto_fix_review":
            add(rule, solo=True, has_value=True)

        elif rule.type == "generate":
            reads = {ACTIVE, rule.column} | when
            for key in ("amount", "currency"):
                if key in rule.params:
                    reads.add(rule.params[key])
            for case in rule.params.get("cases", []) or []:
                reads |= condition_columns(case.get("when"))
            # A column the input lacks is filled without audit entries.
            add(rule, column=rule.column, reads=reads, writes={rule.column},
                audit_column=rule.column,
                audit_action=None if rule.column in filled else "derive")

        else:
            reads = {ACTIVE} | when | {c for c in rule.columns if c != "*"}
            if rule.column:
                reads.add(rule.column)
            for key in ("left", "right", "source"):
                if key in rule.params:
                    reads.add(rule.params[key])
            target = None
            if rule.remediation:
                target = rule.remediation.get("target", rule.column)
                reads.add(target)
            add(rule, column=rule.column, reads=reads,
                writes={target} if target else set(),
                audit_column=target,
                audit_action=(rule.remediation or {}).get("action"))

    return steps


def plan_stages(steps: list[Step]) -> list[list[Step]]:
    """Group consecutive steps that can run in one projection."""

    stages: list[list[Step]] = []
    current: list[Step] = []
    written: set[str] = set()

    for step in steps:
        if step.solo:
            if current:
                stages.append(current)
            stages.append([step])
            current, written = [], set()
            continue
        if step.reads & written or step.writes & written:
            stages.append(current)
            current, written = [], set()
        current.append(step)
        written |= step.writes

    if current:
        stages.append(current)
    return stages


# ============================================================
# Running steps
# ============================================================

@dataclass
class StepOutput:
    """SQL of one step: temporaries computed first (on the stage's input),
    then the column updates and new columns (which may use them)."""

    temps: list[tuple[str, str]] = field(default_factory=list)
    kept: list[tuple[str, str]] = field(default_factory=list)   # like temps, but kept
    updates: dict[str, str] = field(default_factory=dict)
    columns: list[tuple[str, str]] = field(default_factory=list)


class Engine:
    def __init__(self, registry: Registry, data_columns: list[str], filled: set[str]) -> None:
        self.registry = registry
        self.data_columns = data_columns
        self.filled = filled
        self.tokens = null_tokens(registry)

    # ---- one stage: two projections ----

    def value_step(self, step: Step) -> StepOutput:
        """Detect -> remediate -> re-check for one rule on one column."""

        rule = step.rule
        column = step.column
        get = ident
        scope = all_of(get(ACTIVE), condition(get, rule.when))

        if rule.type == "trim_whitespace":
            failing = all_of(scope, whitespace_failures(get(column)))
        elif rule.type == "null_tokens":
            failing = all_of(scope, null_token_failures(get(column), self.tokens))
        else:
            failing = detect(self.registry, rule, get, scope)

        s, f = ident(step.name("s")), ident(step.name("f"))
        out = StepOutput(temps=[(step.name("s"), scope), (step.name("f"), failing)])
        if not rule.remediation:
            out.columns.append((step.code, _code(s, f)))
            return out

        # Usually the rule's own column; derived rules may repair their
        # source column instead.
        target = step.audit_column
        new, changed = remediate(self.registry, dataclasses.replace(rule, column=column), get)
        out.temps += [
            (step.name("h"), flag(changed)),
            (step.name("r"), f"CASE WHEN {f} AND {flag(changed)} THEN {new} "
                             f"ELSE {get(target)} END"),
        ]
        h, r = ident(step.name("h")), ident(step.name("r"))

        if rule.type in ("trim_whitespace", "null_tokens"):
            still = all_of(f, f"NOT {h}")
        else:
            still = detect(self.registry, rule,
                           lambda name: r if name == target else ident(name), s)

        out.updates[target] = r
        out.columns += [
            (step.code, _code(s, f, still)),
            (step.audit, f"CASE WHEN {f} THEN " + _audit(
                lit(target), get(target), r, lit(rule.id), rule.remediation["action"],
                f"CASE WHEN {still} THEN 'not_fixed' ELSE 'remediated' END",
            ) + " END"),
        ]
        return out

    def duplicate_step(self, step: Step) -> StepOutput:
        """Exact copies of an earlier row (on the input text)."""

        active = ident(ACTIVE)
        duplicated = all_of(active, ident(DUP))
        out = StepOutput()
        if step.audit_action:
            out.updates[ACTIVE] = all_of(active, f"NOT {duplicated}")
            out.columns += [
                (step.code, _code(active, duplicated, "false")),
                (step.audit, f"CASE WHEN {duplicated} THEN " + _audit(
                    lit("*"), NULL, NULL, lit(step.rule.id), "drop_duplicates", lit("removed"),
                ) + " END"),
            ]
        else:
            out.columns.append((step.code, _code(active, duplicated)))
        return out

    def generate_step(self, step: Step) -> StepOutput:
        """Fill a generated column from cleaned columns.

        When the input lacks the column it is simply filled. When the input
        has it, values that differ from the computed ones are replaced and
        audited. Rows whose value cannot be computed fail the rule.
        """

        rule = step.rule
        column = ident(rule.column)
        out = StepOutput(temps=[
            (step.name("s"), all_of(ident(ACTIVE), condition(ident, rule.when))),
            (step.name("e"), generated_values(self.registry, rule, ident)),
        ])
        s, e = ident(step.name("s")), ident(step.name("e"))
        missing = all_of(s, f"{e} IS NULL")

        if rule.column in self.filled:
            out.updates[rule.column] = f"CASE WHEN {s} THEN {e} ELSE {column} END"
            out.columns.append((step.code, _code(s, missing)))
            return out

        differs = all_of(s, f"{e} IS NOT NULL", f"NOT {flag(f'{column} = {e}')}")
        out.updates[rule.column] = f"CASE WHEN {differs} THEN {e} ELSE {column} END"
        out.columns += [
            (step.code, f"CAST(CASE WHEN {missing} THEN 3 WHEN {differs} THEN 2 "
                        f"WHEN {s} THEN 1 ELSE 0 END AS TINYINT)"),
            (step.audit, f"CASE WHEN {differs} THEN " + _audit(
                lit(rule.column), column, e, lit(rule.id), "derive", lit("remediated"),
            ) + " END"),
        ]
        return out

    def run_stage(self, df: DataFrame, stage: list[Step]) -> DataFrame:
        outputs = []
        for step in stage:
            if step.rule.type == "duplicate_rows":
                outputs.append(self.duplicate_step(step))
            elif step.rule.type == "generate":
                outputs.append(self.generate_step(step))
            elif step.rule.type == "functional_dependency":
                outputs.append(self.dependency_step(step))
            else:
                outputs.append(self.value_step(step))

        temps = [t for out in outputs for t in out.temps]
        first = temps + [k for out in outputs for k in out.kept]
        if first:
            df = df.selectExpr("*", *[_as(sql, name) for name, sql in first])

        updates = {c: sql for out in outputs for c, sql in out.updates.items()}
        dropped = {name for name, _ in temps}
        return df.selectExpr(
            *[_as(updates[c], c) if c in updates else ident(c)
              for c in df.columns if c not in dropped],
            *[_as(sql, name) for out in outputs for name, sql in out.columns],
        )

    # ---- stages of their own ----

    def dependency_step(self, step: Step) -> StepOutput:
        """Rows whose dependent value differs from the key's most common
        value.

        E.g. a customer_id that shows 'Male' on 20 rows and 'Female' on one:
        the one row fails. On a tie the smallest value counts as the usual
        one. One window per rule: every dependent shares the key.
        """

        rule = step.rule
        determinant, dependents = dependency_columns(self.registry, rule)
        key = ident(determinant)
        scope = all_of(ident(ACTIVE), condition(ident, rule.when))
        window = f"OVER (PARTITION BY {key})"

        out = StepOutput(temps=[(step.name("s"), scope)])
        for j, dependent in enumerate(dependents):
            value = ident(dependent)
            present = all_of(scope, f"{key} IS NOT NULL", f"{value} IS NOT NULL")
            usual = f"mode(CASE WHEN {present} THEN {value} END, true) {window}"
            out.kept.append((f"{step.code}_{j}",
                             f"CAST({all_of(present, flag(f'{value} != {usual}'))} AS TINYINT)"))

        failing = any_of(*[f"{ident(f'{step.code}_{j}')} = 1" for j in range(len(dependents))])
        out.columns.append((step.code, _code(ident(step.name("s")), failing)))
        return out

    def review_stage(self, df: DataFrame, step: Step, earlier: list[Step]) -> DataFrame:
        """Records with many automatic repairs: a person has to confirm them.

        Counts the repairs made so far; min_changes (default 3) or more fail
        the rule. ignore_actions lists remediation actions that do not count.
        """

        rule = step.rule
        minimum = int(rule.params.get("min_changes", 3))
        ignore = set(rule.params.get("ignore_actions") or [])
        counted = [s for s in earlier if s.audit_action and s.audit_action not in ignore]

        def edit(s: Step) -> str:
            return f"{ident(s.audit)}.outcome = 'remediated'"

        count = " + ".join(f"CASE WHEN {edit(s)} THEN 1 ELSE 0 END" for s in counted) or "0"
        # e.g. "3 values: transaction_time '07:19:20 PM' -> '19:19:20'; ..."
        described = ", ".join(
            f"CASE WHEN {edit(s)} THEN concat({ident(s.audit)}.column, ' ''', "
            f"coalesce({ident(s.audit)}.original_value, ''), ''' -> ''', "
            f"coalesce({ident(s.audit)}.cleaned_value, ''), '''') END"
            for s in counted
        )
        df = df.selectExpr("*", _as(f"({count})", "__review_count"))
        value = (f"concat(CAST(`__review_count` AS STRING), ' values: ', "
                 f"{'concat_ws(' + lit('; ') + ', ' + described + ')' if described else lit('')})")
        failing = all_of(ident(ACTIVE), f"`__review_count` >= {minimum}")
        return df.selectExpr(
            *[ident(c) for c in df.columns if c != "__review_count"],
            _as(_code(ident(ACTIVE), failing), step.code),
            _as(value, step.value),
        )


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


def error_column(registry: Registry, rule: Rule) -> str | None:
    """Column an error points at (the one to correct)."""

    if rule.type == "functional_dependency":
        return dependency_columns(registry, rule)[0]
    if rule.type == "compare":
        return rule.params["left"]
    if rule.column:
        return rule.column
    if rule.type == "expression" and rule.columns:
        return rule.columns[0]
    return None


_VALUE = "\x00value\x00"


def message(registry: Registry, rule: Rule, column: str | None, value: str) -> str:
    """The rule's message template with {value} filled in per row (SQL)."""

    context = _Placeholders(
        column=column or "",
        column_type=registry.columns.get(column, {}).get("type", "string") if column else "",
        bounds=_bounds(rule.params),
        left=rule.params.get("left", ""),
        right=rule.params.get("right", ""),
        operator=rule.params.get("operator", ""),
        source=rule.params.get("source", ""),
        reference=(rule.params.get("reference") or {}).get("path", ""),
        determinant=column or "",
        name=rule.name,
        value=_VALUE,
    )
    parts = rule.message_template.format_map(context).split(_VALUE)
    pieces = [lit(parts[0])]
    for part in parts[1:]:
        pieces += [f"coalesce(CAST({value} AS STRING), '')", lit(part)]
    return f"concat({', '.join(pieces)})"


# ============================================================
# Validation
# ============================================================

@dataclass
class Validation:
    """Outcome of validating a batch (df is persisted until release())."""

    df: DataFrame
    data_columns: list[str]
    steps: list[Step]
    registry: Registry
    rule_stats: list[dict[str, Any]]
    counts: dict[str, int]
    columns: dict[str, dict[str, int]]

    def _outputs(self) -> list[str]:
        return [f"{ident(f'__o{j}')} AS {ident(c)}" for j, c in enumerate(self.data_columns)]

    def _rows(self, condition: str | None, focus: bool) -> DataFrame:
        if focus:
            condition = ident(FOCUS) if condition is None else all_of(condition, ident(FOCUS))
        return self.df if condition is None else self.df.filter(condition)

    def valid(self, focus: bool = False) -> DataFrame:
        """record_id, values, dq_warnings of the accepted records (focus:
        only the focus rows)."""

        return self._rows(ident(ACCEPTED), focus).selectExpr(
            f"{ident(RID)} AS record_id", *self._outputs(), f"{ident(WARNINGS)} AS dq_warnings",
        )

    def quarantine(self, focus: bool = False) -> DataFrame:
        """record_id, values, dq_errors, dq_warnings of the quarantined ones."""

        return self._rows(ident(QUARANTINED), focus).selectExpr(
            f"{ident(RID)} AS record_id", *self._outputs(),
            f"{ident(ERRORS)} AS dq_errors", f"{ident(WARNINGS)} AS dq_warnings",
        )

    def errors(self, quarantined_only: bool = False, focus: bool = False) -> DataFrame:
        """One row per failed rule and record (layout.ERROR_COLUMNS)."""

        entries = []
        for step in self.steps:
            rule = step.rule
            column = error_column(self.registry, rule)
            value = (
                ident(step.value) if step.has_value
                else ident(column) if column in self.data_columns
                else NULL
            )
            entries.append(
                f"CASE WHEN {ident(step.code)} = 3 THEN named_struct("
                f"'rule_id', {lit(rule.id)}, 'rule_name', {lit(rule.name)}, "
                f"'error_code', {lit(rule.code)}, 'error_category', {lit(rule.error_category)}, "
                f"'dimension', {lit(rule.dimension)}, 'severity', {lit(rule.severity)}, "
                f"'column', CAST({lit(column)} AS STRING), 'value', CAST({value} AS STRING), "
                f"'message', {message(self.registry, rule, column, value)}) END"
            )
        rows = self._rows(ident(QUARANTINED) if quarantined_only else None, focus)
        return rows.selectExpr(
            f"{ident(RID)} AS record_id",
            f"explode(filter(array({', '.join(entries)}), e -> e IS NOT NULL)) AS e",
        ).selectExpr("record_id", "e.*")

    def audit(self, focus: bool = False) -> DataFrame:
        """Every automatic repair (layout.AUDIT_COLUMNS)."""

        names = [s.audit for s in self.steps if s.audit_action]
        names += [c for c in self.df.columns if c.startswith("__n")]
        return self._rows(None, focus).selectExpr(
            f"{ident(RID)} AS record_id",
            f"explode(filter(array({', '.join(ident(n) for n in names)}), "
            f"e -> e IS NOT NULL)) AS e",
        ).selectExpr("record_id", *[f"e.{f}" for f in AUDIT_FIELDS])

    def release(self) -> None:
        self.df.unpersist()


def validate_frame(
    df: DataFrame,
    registry: Registry,
    log: Callable[[str], None] = print,
) -> Validation:
    """Run every rule on a batch and split it into valid and quarantine.

    df: __rid, __row (input order), the data columns as text (null for
    missing) and optionally __focus (rows counted in the rule statistics;
    the others are context for cross-row rules).
    """

    input_columns = [c for c in df.columns if not c.startswith("__")]
    if FOCUS not in df.columns:
        df = df.withColumn(FOCUS, F.lit(True))

    # Generated columns the input does not provide: filled by their
    # generate rule, in schema order.
    filled = {
        c for c, spec in registry.columns.items()
        if spec.get("generated") and c not in input_columns
    }
    data_columns = [c for c in registry.columns if c in input_columns or c in filled]
    data_columns += [c for c in input_columns if c not in registry.columns]

    missing_in_input = df.selectExpr(*[
        f"sum(CASE WHEN {ident(c)} IS NULL THEN 1 ELSE 0 END) AS m{j}"
        for j, c in enumerate(input_columns)
    ]).collect()[0]

    partition = ", ".join(ident(c) for c in input_columns)
    df = df.selectExpr(
        ident(RID), ident(ROW), ident(FOCUS), f"true AS {ident(ACTIVE)}",
        f"row_number() OVER (PARTITION BY {partition} ORDER BY {ident(ROW)}) > 1 AS {ident(DUP)}",
        *[ident(c) if c in input_columns else f"{NULL} AS {ident(c)}" for c in data_columns],
    )

    engine = Engine(registry, data_columns, filled)
    steps = build_steps(registry, data_columns, filled)
    stages = plan_stages(steps)
    log(f"  {len(steps)} steps in {len(stages)} stages")

    clock = time.perf_counter()
    done: list[Step] = []
    for number, stage in enumerate(stages, start=1):
        step = stage[0]
        if step.rule.type == "auto_fix_review":
            df = engine.review_stage(df, step, done)
        else:
            df = engine.run_stage(df, stage)
        done += stage
        if number % CHECKPOINT_EVERY == 0:
            # On disk: a wide batch would not fit in memory several times.
            df = df.localCheckpoint(eager=True, storageLevel=StorageLevel.DISK_ONLY)
    df = split_records(df, registry, steps, data_columns)
    df = df.persist(StorageLevel.DISK_ONLY)

    rule_stats, counts, columns = collect_stats(df, steps, data_columns, input_columns,
                                                missing_in_input)
    log(f"  rules applied in {time.perf_counter() - clock:.1f}s")
    for stats in rule_stats:
        if stats["detected"]:
            log(f"  {stats['rule_id']:<45} detected {stats['detected']:>8}  "
                f"unresolved {stats['unresolved']:>8}")

    return Validation(df=df, data_columns=data_columns, steps=steps, registry=registry,
                      rule_stats=rule_stats, counts=counts, columns=columns)


# ============================================================
# Accept / quarantine
# ============================================================

def split_records(
    df: DataFrame, registry: Registry, steps: list[Step], data_columns: list[str],
) -> DataFrame:
    """Decide accepted vs quarantined rows and build both outputs."""

    actions = registry.severity_actions

    def failed(s: Step) -> str:
        return f"{ident(s.code)} = 3"

    def id_list(selected: list[Step]) -> str:
        """';'-joined rule ids per row; null when none failed."""
        if not selected:
            return NULL
        parts = ", ".join(f"CASE WHEN {failed(s)} THEN {lit(s.rule.id)} END" for s in selected)
        return f"nullif(concat_ws(';', {parts}), '')"

    quarantine_steps = [s for s in steps if actions[s.rule.severity] == "quarantine"]
    flag_steps = [s for s in steps if actions[s.rule.severity] == "accept_with_flag"]

    quarantined = all_of(ident(ACTIVE), any_of(*[failed(s) for s in quarantine_steps]))
    df = df.selectExpr(
        "*",
        _as(quarantined, QUARANTINED),
        _as(id_list(flag_steps), WARNINGS),
        _as(id_list(quarantine_steps), ERRORS),
    )
    accepted = all_of(ident(ACTIVE), f"NOT {ident(QUARANTINED)}")
    df = df.selectExpr("*", _as(accepted, ACCEPTED))
    formats = registry.dataset.get("canonical_formats", {})

    outputs = []
    for j, column in enumerate(data_columns):
        value = ident(column)

        # Values still invalid in accepted rows are nulled so the accepted
        # data always loads with its declared types.
        nulling = [s for s in steps
                   if s.rule.on_accept == "set_null" and s.rule.column == column]
        if nulling:
            hit = all_of(ident(ACCEPTED), any_of(*[failed(s) for s in nulling]),
                         f"{value} IS NOT NULL")
            rule_id = "coalesce(" + ", ".join(
                f"CASE WHEN {failed(s)} THEN {lit(s.rule.id)} END" for s in nulling
            ) + ")"
            outputs.append(_as(f"CASE WHEN {hit} THEN " + _audit(
                lit(column), value, NULL, rule_id, "set_null", lit("nullified_in_accepted"),
            ) + " END", f"__n{j}"))
            value = f"CASE WHEN {hit} THEN {NULL} ELSE {value} END"

        # Canonical text for dates and times; values that cannot be read
        # keep their working text so quarantined records show what is wrong.
        spec = registry.columns.get(column)
        if spec:
            text = canonical_text(parse_values(value, spec), spec, formats)
            if text is not None:
                value = f"coalesce({text}, {value})"
        outputs.append(_as(value, f"__o{j}"))

    return df.selectExpr("*", *outputs)


def new_stats(rule: Rule) -> dict[str, Any]:
    return {**rule.describe(), "checked": 0, "detected": 0, "remediated": 0, "unresolved": 0}


def collect_stats(
    df: DataFrame,
    steps: list[Step],
    data_columns: list[str],
    input_columns: list[str],
    missing_in_input: Any,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, dict[str, int]]]:
    """Rule statistics (over the focus rows), counts and missing values,
    in one pass."""

    focus = ident(FOCUS)

    def count(condition: str) -> str:
        return f"sum(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    aggregates = [
        "count(1) AS input_rows",
        f"{count(ident(ACTIVE))} AS processed",
        f"{count(ident(ACCEPTED))} AS accepted",
        f"{count(ident(QUARANTINED))} AS quarantined",
        f"{count(all_of(ident(ACCEPTED), f'{ident(WARNINGS)} IS NOT NULL'))} AS flagged",
    ]
    for s in steps:
        code = ident(s.code)
        aggregates += [
            f"{count(all_of(focus, f'{code} >= 1'))} AS k{s.index}",
            f"{count(all_of(focus, f'{code} >= 2'))} AS d{s.index}",
            f"{count(all_of(focus, f'{code} = 3'))} AS u{s.index}",
        ]
        for j in range(len(s.fd_columns)):
            aggregates.append(f"sum({ident(f'{s.code}_{j}')}) AS f{s.index}_{j}")
    for c in input_columns:
        j = data_columns.index(c)
        aggregates.append(
            f"{count(all_of(ident(ACCEPTED), f'{ident(f'__o{j}')} IS NULL'))} AS a{j}"
        )

    row = df.selectExpr(*aggregates).collect()[0]

    by_rule: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for s in steps:
        rule = s.rule
        if rule.id not in by_rule:
            by_rule[rule.id] = new_stats(rule)
            order.append(rule.id)
        stats = by_rule[rule.id]
        stats["checked"] += row[f"k{s.index}"]
        stats["detected"] += row[f"d{s.index}"]
        stats["unresolved"] += row[f"u{s.index}"]
        stats["remediated"] = stats["detected"] - stats["unresolved"]
        if s.fd_columns:
            stats["conflicts_by_column"] = {
                c: int(row[f"f{s.index}_{j}"] or 0)
                for j, c in enumerate(s.fd_columns) if row[f"f{s.index}_{j}"]
            }

    counts = {k: int(row[k]) for k in ("input_rows", "processed", "accepted",
                                        "quarantined", "flagged")}
    columns = {
        c: {
            "missing_in_input": int(missing_in_input[f"m{i}"] or 0),
            "missing_in_accepted": int(row[f"a{data_columns.index(c)}"] or 0),
        }
        for i, c in enumerate(input_columns)
    }
    return [by_rule[r] for r in order], counts, columns


# ============================================================
# Results
# ============================================================

def summarize(result: Validation, run: dict[str, Any]) -> dict[str, Any]:
    counts = result.counts
    input_rows, processed = counts["input_rows"], counts["processed"]
    accepted, quarantined = counts["accepted"], counts["quarantined"]

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

    outcomes = [
        {"action": r["action"], "outcome": r["outcome"], "count": r["count"]}
        for r in result.audit().groupBy("action", "outcome").count()
        .orderBy("action", "outcome").collect()
    ]
    errors_by_category = [
        {"error_category": r["error_category"], "error_code": r["error_code"],
         "count": r["count"]}
        for r in result.errors(quarantined_only=True)
        .groupBy("error_category", "error_code").count()
        .orderBy("error_category", "error_code").collect()
    ]

    return {
        **run,
        "counts": {
            "input_rows": input_rows,
            "duplicate_rows_removed": input_rows - processed,
            "processed_rows": processed,
            "accepted_rows": accepted,
            "accepted_with_warnings_rows": counts["flagged"],
            "quarantined_rows": quarantined,
            "acceptance_rate": rate(accepted, processed),
            "quarantine_rate": rate(quarantined, processed),
            "remediated_values": sum(
                o["count"] for o in outcomes if o["outcome"] == "remediated"
            ),
        },
        "dimensions": dimensions,
        "quarantine_errors": errors_by_category,
        "remediation_outcomes": outcomes,
        "rules": result.rule_stats,
        "columns": result.columns,
    }


# ============================================================
# Main
# ============================================================

def read_source(path: Path, encoding: str, delimiter: str) -> DataFrame:
    """An ad hoc CSV file: every column as text, empty cells null, with
    record ids ROW_000001, ... in file order."""

    df = (
        spark_io.session().read
        .option("header", True).option("inferSchema", False)
        .option("encoding", encoding.replace("-sig", "")).option("sep", delimiter)
        .option("multiLine", True).option("escape", '"')
        .csv(str(path.resolve()))
    )
    # A UTF-8 byte order mark ends up in the first column name.
    df = df.toDF(*[c.lstrip("﻿") for c in df.columns])
    order = F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    df = df.select(order.alias(ROW), *[F.when(col(c) != "", col(c)).alias(c) for c in df.columns])
    width = max(6, len(str(df.count())))
    return df.select(F.concat(F.lit("ROW_"), F.lpad(col(ROW).cast("string"), width, "0"))
                     .alias(RID), "*")


def read_ingestion(manifest: dict[str, Any]) -> DataFrame:
    """Rows of a bronze ingestion; empty values become null."""

    columns = manifest["columns"]
    if not manifest["table_name"]:
        raise bronze.BronzeError(
            f"{manifest['ingestion_id']} was stored before per-dataset tables "
            "(bronze.raw_record); ingest the source again."
        )
    query = (
        "SELECT row_number, record_id, "
        + ", ".join(db.quote(c) for c in columns)
        + f" FROM {db.quote('bronze')}.{db.quote(manifest['table_name'])}"
        + f" WHERE ingestion_id = {db.literal(manifest['ingestion_id'])}"
    )
    df = spark_io.read(query, predicates=spark_io.hash_predicates("record_id"))
    return df.select(
        col("record_id").alias(RID), col("row_number").cast("long").alias(ROW),
        *[F.when(col(c) != "", col(c)).alias(c) for c in columns],
    )


def check_columns(columns: list[str], registry: Registry) -> dict[str, list]:
    """Schema drift: missing columns stop the run, extra ones are kept.

    Generated columns may be missing; the engine fills them.
    """

    expected = [c for c, spec in registry.columns.items() if not spec.get("generated")]
    missing = [c for c in expected if c not in columns]
    extra = [c for c in columns if c not in registry.columns]
    reserved = [c for c in columns if c.startswith("__")]

    if missing:
        raise ConfigError(f"Input is missing schema columns: {missing}")
    if reserved:
        raise ConfigError(f"Column names starting with '__' are reserved: {reserved}")

    return {"missing_columns": missing, "extra_columns": extra}


def store_run(layout: Layout, result: Validation, summary: dict[str, Any], source: str) -> None:
    """Write round 1 of a run. The round result goes last: a run without
    it is incomplete and is replaced by the next validation."""

    run = {"dataset": layout.dataset, "run_id": layout.run_id}
    layout.delete()
    layout.start(result.data_columns, source)

    valid_table = layout.prepare_records("valid")
    quarantine_table = layout.prepare_records("quarantine", 1)
    spark_io.write(result.valid().withColumn("run_id", F.lit(layout.run_id)), valid_table)
    spark_io.write(
        result.quarantine().withColumn("run_id", F.lit(layout.run_id))
        .withColumn("round", F.lit(1)),
        quarantine_table,
    )
    stamp = {**run, "round": 1}
    spark_io.write(_with(result.errors(quarantined_only=True), stamp), "dq.quarantine_error")
    spark_io.write(_with(result.audit(), stamp), "dq.remediation_audit")

    timestamp = now_utc()
    ledger = result.df.filter(col(QUARANTINED)).select(
        *[F.lit(v).alias(k) for k, v in run.items()],
        col(RID).alias("record_id"), F.lit("OPEN").alias("status"),
        F.lit(1).alias("round"), F.lit(0).alias("attempts"),
    )
    spark_io.write(ledger.withColumn("updated_at", F.lit(timestamp)), "dq.quarantine_status")
    spark_io.write(ledger.withColumn("changed_at", F.lit(timestamp)), "dq.quarantine_history")

    layout.save_result(1, summary)


def _with(df: DataFrame, constants: dict[str, Any]) -> DataFrame:
    return df.select(*[F.lit(v).alias(k) for k, v in constants.items()], "*")


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
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES, help="Quality rules YAML.")
    parser.add_argument(
        "--schema", type=Path, default=None,
        help="Schema registry YAML (default: the one named in the rules).",
    )
    parser.add_argument("--run-id", default=None,
                        help="Run id for --input (default: UTC timestamp).")
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
        manifest = None
    else:
        with bronze.connect() as conn:
            manifest = bronze.find_ingestion(conn, name, args.ingestion)
        source = f"bronze.{manifest['table_name']} ({manifest['source_file']})"
        run_id = manifest["ingestion_id"]

    layout = Layout(name, run_id)
    if layout.exists() and not args.force:
        raise ConfigError(
            f"Run {run_id} is already validated; use --force to start "
            "over (this removes its quarantine rounds and corrections)."
        )

    print(f"Run {run_id}: {source}")
    print(f"Loaded {len(registry.all_rules())} rules.")

    raw = (
        read_ingestion(manifest) if manifest
        else read_source(args.input, source_cfg.get("encoding", "utf-8-sig"),
                         source_cfg.get("delimiter", ","))
    )
    drift = check_columns([c for c in raw.columns if c not in (RID, ROW)], registry)
    if drift["extra_columns"]:
        print(f"Columns not in the schema (kept, not validated): {drift['extra_columns']}")

    result = validate_frame(raw, registry)

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
        "config": {
            "rules_file": str(args.rules),
            "schema_version": registry.schema.get("version"),
            "rules_version": registry.rules_config.get("version"),
            "rule_count": len(registry.all_rules()),
            "engine": "pyspark",
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
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    summary["duration_seconds"] = round(time.perf_counter() - clock, 2)

    store_run(layout, result, summary, source)
    result.release()

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
    finally:
        spark_io.stop()
