"""Rule detection: which rows break a rule (Spark SQL expressions).

Every check is a boolean SQL expression (true = the row fails) and only
ever flags rows inside the given scope (active rows that match the rule's
"when" condition). Column values are read through a resolver, get(name)
-> SQL, so a check can be evaluated on repaired values before they are
stored.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Callable

from values import (
    NULL,
    all_of,
    any_of,
    case,
    flag,
    format_number,
    format_temporal,
    full_match,
    ident,
    is_in,
    lit,
    parse_values,
    resolve_bound,
    strip,
)

if TYPE_CHECKING:
    from registry import Registry, Rule

Getter = Callable[[str], str]

OPERATORS = {">=": ">=", ">": ">", "<=": "<=", "<": "<", "==": "=", "!=": "!="}


# ============================================================
# Conditions ("when")
# ============================================================

def condition(get: Getter, spec: dict[str, Any] | None) -> str:
    """Rows matching a rule's applicability condition."""

    if not spec:
        return "true"

    if "all" in spec:
        return all_of(*[condition(get, part) for part in spec["all"]])

    if "any" in spec:
        return any_of(*[condition(get, part) for part in spec["any"]])

    if "not" in spec:
        return f"(NOT {condition(get, spec['not'])})"

    values = get(spec["column"])

    if "in" in spec:
        return flag(is_in(values, spec["in"]))
    if "not_in" in spec:
        return all_of(f"{values} IS NOT NULL", f"NOT {flag(is_in(values, spec['not_in']))}")
    if "equals" in spec:
        return flag(f"{values} = {lit(str(spec['equals']))}")
    if "is_null" in spec:
        return f"({values} IS {'' if spec['is_null'] else 'NOT '}NULL)"
    if "not_null" in spec:
        return f"({values} IS {'NOT ' if spec['not_null'] else ''}NULL)"

    raise ValueError(f"Unsupported condition: {spec}")


def condition_columns(spec: dict[str, Any] | None) -> set[str]:
    if not spec:
        return set()
    columns: set[str] = set()
    for key in ("all", "any"):
        for part in spec.get(key, []):
            columns |= condition_columns(part)
    if "not" in spec:
        columns |= condition_columns(spec["not"])
    if "column" in spec:
        columns.add(spec["column"])
    return columns


# ============================================================
# Row-level checks
# ============================================================

def typed(registry: Registry, get: Getter, column: str) -> str:
    return parse_values(get(column), registry.columns.get(column, {"type": "string"}))


def _range_failures(values: str, params: dict[str, Any], column_type: str) -> str:
    low = resolve_bound(params.get("min"), column_type)
    high = resolve_bound(params.get("max"), column_type)
    out = []

    if low is not None:
        out.append(f"{values} {'<=' if params.get('exclusive_min') else '<'} {lit(low)}")
    if high is not None:
        out.append(f"{values} {'>=' if params.get('exclusive_max') else '>'} {lit(high)}")

    return all_of(flag(any_of(*out)), f"{values} IS NOT NULL")


def _with_offset(values: str, offset: dict[str, Any] | None) -> str:
    """date + {years, months, days} (calendar arithmetic, as DateOffset)."""

    if not offset:
        return values
    months = int(offset.get("years", 0)) * 12 + int(offset.get("months", 0))
    if months:
        values = f"add_months({values}, {months})"
    if offset.get("days"):
        values = f"date_add({values}, {int(offset['days'])})"
    return values


def expression_sql(registry: Registry, rule: Rule, get: Getter) -> str:
    """The rule's expression with every column replaced by its typed value."""

    text = rule.params["expression"]
    typed_columns = {c: typed(registry, get, c) for c in rule.columns}
    pattern = "|".join(re.escape(c) for c in sorted(typed_columns, key=len, reverse=True))
    return re.sub(
        rf"(?<![\w`])(?:{pattern})(?![\w`])",
        lambda m: f"({typed_columns[m.group(0)]})", text,
    )


def detect(registry: Registry, rule: Rule, get: Getter, scope: str) -> str:
    """Rows in scope that break a single-column or cross-column rule."""

    kind = rule.type
    column = rule.column
    values = get(column) if column else None

    if kind == "not_null":
        failing = f"({values} IS NULL)"

    elif kind == "is_null":
        failing = f"({values} IS NOT NULL)"

    elif kind == "type":
        failing = all_of(f"{values} IS NOT NULL", f"{typed(registry, get, column)} IS NULL")

    elif kind == "pattern":
        failing = all_of(f"{values} IS NOT NULL",
                         f"NOT {flag(full_match(values, rule.params['pattern']))}")

    elif kind in ("allowed_values", "reference"):
        # reference: values loaded from the lookup file at config load.
        failing = all_of(f"{values} IS NOT NULL",
                         f"NOT {flag(is_in(values, rule.params['values']))}")

    elif kind == "expression":
        # e.g. 'abs(amount_egp - transaction_amount) <= 0.01' over typed
        # values; rows with a missing input are not judged.
        present = all_of(*[f"{typed(registry, get, c)} IS NOT NULL" for c in rule.columns])
        holds = f"CAST(({expression_sql(registry, rule, get)}) AS BOOLEAN)"
        failing = all_of(present, f"NOT {flag(holds)}")

    elif kind == "range":
        failing = _range_failures(
            typed(registry, get, column), rule.params,
            registry.columns.get(column, {}).get("type", "decimal"),
        )

    elif kind == "unique":
        present = all_of(scope, f"{values} IS NOT NULL")
        count = f"sum(CASE WHEN {present} THEN 1 END) OVER (PARTITION BY {values})"
        failing = all_of(present, f"{count} > 1")

    elif kind == "compare":
        left = typed(registry, get, rule.params["left"])
        right = _with_offset(typed(registry, get, rule.params["right"]),
                             rule.params.get("offset"))
        holds = f"{left} {OPERATORS[rule.params['operator']]} {right}"
        failing = all_of(f"{left} IS NOT NULL", f"{right} IS NOT NULL", f"NOT {flag(holds)}")

    elif kind == "derived":
        expected = derived_values(registry, rule, get)
        actual = format_temporal(typed(registry, get, column), rule.params["format"])
        failing = all_of(f"{expected} IS NOT NULL",
                         any_of(f"{actual} IS NULL", flag(f"{actual} != {expected}")))

    else:
        raise ValueError(f"detect() does not handle {kind}")

    return all_of(scope, flag(failing))


def derived_values(registry: Registry, rule: Rule, get: Getter) -> str:
    """Expected value of a derived column, from its source column."""

    return format_temporal(typed(registry, get, rule.params["source"]), rule.params["format"])


def generated_values(registry: Registry, rule: Rule, get: Getter) -> str:
    """Values of a generated column, computed from cleaned columns.

    convert_currency  amount x rate of its currency, e.g. 1000 USD at
                      52.40 -> '52400'
    case              value of the first case whose condition matches,
                      else the default (null without one)

    Null where the value cannot be computed (unknown currency, no match).
    """

    method = rule.params["method"]

    if method == "convert_currency":
        amount = typed(registry, get, rule.params["amount"])
        currency = get(rule.params["currency"])
        rate = case(*[(f"{currency} = {lit(str(code))}", lit(float(value)))
                      for code, value in rule.params["rates"].items()],
                    otherwise="CAST(NULL AS DOUBLE)")
        decimals = int(rule.params.get("decimals", 2))
        return format_number(f"round({amount} * {rate}, {decimals})")

    if method == "case":
        default = lit(str(rule.params["default"])) if "default" in rule.params else NULL
        return case(*[(condition(get, c.get("when")), lit(str(c["value"])))
                      for c in rule.params["cases"]], otherwise=default)

    raise ValueError(f"Rule {rule.id}: unknown generate method {method}")


# ============================================================
# Preprocessing checks (one per column)
# ============================================================

def whitespace_failures(values: str) -> str:
    return all_of(f"{values} IS NOT NULL", flag(f"{values} != {strip(values)}"))


def null_token_failures(values: str, tokens: set[str]) -> str:
    return all_of(f"{values} IS NOT NULL", flag(is_in(f"lower({strip(values)})", sorted(tokens))))


def null_tokens(registry: Registry) -> set[str]:
    """Null placeholders from the schema registry, compared casefolded."""

    tokens = registry.dataset.get("null_tokens", [])
    return {str(t).strip().casefold() for t in tokens} - {""}


# ============================================================
# Relationships
# ============================================================

def dependency_columns(registry: Registry, rule: Rule) -> tuple[str, list[str]]:
    """Determinant and dependent columns of a functional dependency."""

    schema = registry.schema

    if "entity" in rule.params:
        entity = schema["entities"][rule.params["entity"]]
        dependents = list(entity.get("attributes", []))
        dependents += list((entity.get("references") or {}).values())
        return entity["key"], dependents

    dependency = schema["dependencies"][rule.params["dependency"]]
    return dependency["determinant"], list(dependency["dependents"])


__all__ = ["Getter", "condition", "condition_columns", "dependency_columns", "detect",
           "derived_values", "flag", "generated_values", "ident", "null_token_failures",
           "null_tokens", "typed", "whitespace_failures"]
