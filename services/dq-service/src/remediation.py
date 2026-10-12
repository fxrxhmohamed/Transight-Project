"""Remediation actions: try to repair values that failed a rule (Spark SQL).

Each action returns SQL expressions for the repaired value of the target
column (the rule's column unless the remediation names a "target") and
for whether the action actually changed it. The engine applies them only
to failing rows; values it cannot repair are left untouched, the rule is
re-checked afterwards and the outcome goes to the audit trail.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from checks import Getter, derived_values, typed
from values import (
    ISO_DATE_FORMAT,
    NULL,
    all_of,
    any_of,
    empty_to_null,
    flag,
    format_number,
    format_temporal,
    is_in,
    java_regex,
    java_replacement,
    lit,
    lookup,
    normalize_key,
    normalize_one,
    parse_temporal,
    recover_numbers,
    repair_times,
    rlike,
    strip,
    to_number,
)

if TYPE_CHECKING:
    from registry import Registry, Rule

# {0}, {0:0>7}: the templates reformat_id accepts.
TEMPLATE_FIELD = re.compile(r"\{0(?::(.)?>(\d+))?\}")


def remediate(registry: Registry, rule: Rule, get: Getter) -> tuple[str, str]:
    """Repaired value and whether it changed, for the rule's column."""

    options = rule.remediation or {}
    action = options["action"]
    values = get(rule.column)

    if action == "trim":
        new = empty_to_null(strip(values))
        return new, any_of(flag(f"{values} != {new}"), f"{new} IS NULL")

    if action == "set_null":
        changed = (
            _matches_any(registry, rule.column, values, options["values"])
            if "values" in options else "true"
        )
        return NULL, changed

    if action == "canonicalize":
        allowed = rule.params.get("values") or registry.columns.get(rule.column, {}).get(
            "allowed_values", [],
        )
        mapping = {normalize_one(v): str(v) for v in allowed}
        mapping.update({
            normalize_one(k): str(v) for k, v in (options.get("map") or {}).items()
        })
        new = lookup(mapping, normalize_key(values))
        return new, all_of(f"{new} IS NOT NULL", flag(f"{new} != {values}"))

    if action == "parse_number":
        numbers = recover_numbers(values)
        return format_number(numbers), f"({numbers} IS NOT NULL)"

    if action == "parse_date":
        dates = parse_temporal(values, list(options.get("formats", [])))
        return format_temporal(dates, ISO_DATE_FORMAT), f"({dates} IS NOT NULL)"

    if action == "parse_time":
        new = repair_times(values)
        return new, f"({new} IS NOT NULL)"

    if action == "replace":
        # Ordered regex substitutions, e.g. 'BUSINESS_REVIEW_Karim' ->
        # 'Karim', 'O?Connor' -> "O'Connor", '+201012345678' ->
        # '01012345678'. Nothing left -> null.
        new = values
        for step in options.get("substitutions", []):
            new = (f"regexp_replace({new}, {lit(java_regex(step['pattern']))}, "
                   f"{lit(java_replacement(step.get('replace', '')))})")
        new = empty_to_null(strip(new))
        return new, any_of(f"{new} IS NULL", flag(f"{new} != {values}"))

    if action == "reformat_id":
        branches = []
        for pattern in options.get("patterns", []):
            regex = "(?i)" + java_regex(pattern)
            group = f"regexp_extract({values}, {lit(regex)}, 1)"
            branches.append((flag(rlike(values, regex)),
                             _fill_template(options["template"], group)))
        new = (
            "CASE " + " ".join(f"WHEN {c} THEN {v}" for c, v in branches) + " END"
            if branches else NULL
        )
        return new, f"({new} IS NOT NULL)"

    if action == "derive":
        new = derived_values(registry, rule, get)
        return new, f"({new} IS NOT NULL)"

    if action == "swap_day_month":
        return _swap_day_month(registry, rule, get)

    raise ValueError(f"Rule {rule.id}: unsupported remediation {action}")


def _fill_template(template: str, group: str) -> str:
    """'ACC{0:0>7}' with '66821' -> 'ACC0066821' (str.format semantics:
    padding never shortens a longer value)."""

    parts: list[str] = []
    position = 0
    for field in TEMPLATE_FIELD.finditer(template):
        parts.append(lit(template[position:field.start()]))
        fill, width = field.group(1), field.group(2)
        if width:
            parts.append(f"CASE WHEN length({group}) >= {int(width)} THEN {group} "
                         f"ELSE lpad({group}, {int(width)}, {lit(fill or ' ')}) END")
        else:
            parts.append(group)
        position = field.end()
    rest = template[position:]
    if "{" in rest or "}" in rest:
        raise ValueError(f"Unsupported reformat_id template {template!r}")
    parts.append(lit(rest))
    return f"concat({', '.join(parts)})"


def _swap_day_month(registry: Registry, rule: Rule, get: Getter) -> tuple[str, str]:
    """Re-read the source date day-first when that agrees with the
    derived column.

    '1/3/2025' read as 3 January disagrees with month '2025-03'; read as
    1 March it agrees, so the date becomes 2025-03-01.
    """

    dates = typed(registry, get, rule.params["source"])
    swapped = f"make_date(year({dates}), dayofmonth({dates}), month({dates}))"
    recorded = format_temporal(typed(registry, get, rule.column), rule.params["format"])
    changed = all_of(f"{swapped} IS NOT NULL",
                     flag(f"{format_temporal(swapped, rule.params['format'])} = {recorded}"))
    return format_temporal(swapped, ISO_DATE_FORMAT), changed


def _matches_any(registry: Registry, column: str, values: str, targets: list) -> str:
    """Values equal to any target (numerically for numeric columns)."""

    if registry.columns.get(column, {}).get("type") in ("integer", "decimal"):
        numbers = ", ".join(lit(float(t)) for t in targets)
        return flag(f"{to_number(values)} IN ({numbers})")
    return flag(is_in(normalize_key(values), [normalize_one(t) for t in targets]))
