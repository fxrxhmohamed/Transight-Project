"""Generic, read-only CSV data profiler (exploration only).

Scans one CSV file or every CSV under a folder and writes one JSON profile
per file. Source data is never modified.

The profiler only describes what is in the data. It does not decide what
is acceptable: there are no severities, thresholds or pass/fail results.
Use the profiles to write the data quality rules, which live in a
separate rules file.

Tuned for the Egypt banking transactions extract
(data/raw/banking/banking_transactions.csv: one denormalised row per
transaction with customer, account, bank and branch attributes), which
contains:

    * dates written in several layouts (2022-12-19, 27-09-2022,
      2024/05/01, 14/08/2025, 07 Sep 2023); the day/month order is worked
      out per layout and separator
    * times in 24-hour and 12-hour form (15:07:37, 07:19:20 PM)
    * category spellings that differ by case, spacing or underscores
      ('Mobile App', 'MOBILE APP', 'Mobile_App', '  Mobile App  ') and
      catch-all labels ('other')
    * columns that are only filled for some categories (merchant only for
      purchases and bill payments, failure reason only for failed
      transactions, login result only for digital channels)
    * entity attributes repeated on every row (customer, account, branch),
      with placeholder values shared by many customers ('invalid-email',
      'abc#123')
    * repeated transaction ids whose rows differ in a few columns
    * placeholder numbers at the edge of the range (credit_score 0)

Usage examples
--------------
    # Banking defaults
    python src/profiler.py

    # Any other dataset
    python src/profiler.py --input data/raw/sales --output data/profiling/sales \
        --dataset-name sales

    # Custom null tokens and delimiter, no cross-column checks
    python src/profiler.py --null-tokens NULL N/A unknown --delimiter ";" \
        --skip-relationships

    # An ingestion in the bronze layer: the profile is stored with it
    python src/profiler.py --ingestion ING_20261011_001
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# The bronze layer client lives with the ingestion service (the Docker
# image copies it next to this file).
sys.path.append(str(Path(__file__).resolve().parents[2] / "ingestion-service" / "src"))
import bronze  # noqa: E402


# ============================================================
# Defaults (all can be overridden from the command line)
# ============================================================

def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "banking"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "profiling"
DEFAULT_DATASET_NAME = "banking_transactions"

# Text that means "no value". Compared after strip + casefold.
DEFAULT_NULL_TOKENS = (
    "null", "none", "nan", "n/a", "na", "#n/a", "nil", "-", "--", "?",
)

# Share of non-missing values that must parse for a type to be assigned.
TYPE_THRESHOLD = 0.80

# A date- or time-like column name lowers the bar, so badly formatted
# columns are still profiled (and flagged) as dates or times.
NAME_HINT_THRESHOLD = 0.50

# Below these limits a text column is called "categorical".
CATEGORICAL_MAX_UNIQUE = 50
CATEGORICAL_MAX_UNIQUE_RATIO = 0.05

DATE_SAMPLE_SIZE = 1000

# How many patterns / values / groups are listed in a profile.
TOP_N = 10

# Values beyond this many IQRs outside the quartiles are counted.
FAR_OUTLIER_IQR = 3.0

# A min/max value that is a far outlier and holds at least this share of
# the values is reported as a possible placeholder (e.g. credit_score 0).
REPEATED_EXTREME_MIN_SHARE = 0.001

# Catch-all category labels, compared after normalize_text.
GENERIC_CATEGORY_VALUES = frozenset({
    "other", "others", "unknown", "misc", "miscellaneous", "undefined",
    "unspecified", "not specified", "tbd",
})

# Cross-column checks.
# An identifier unique on at least this share of rows is a near key: its
# repeats are reported as duplicate keys, not as an entity.
NEAR_KEY_MIN_SHARE = 0.99
# A column is an attribute of a key when at least this share of its rows
# agrees with the key's most common value...
ENTITY_ATTRIBUTE_MIN_SHARE = 0.95
# ...and knowing the key removes most of the disagreement: the rows that
# disagree are at most this share of those that would disagree with the
# column's overall most common value. Stops skewed columns (currency is
# almost always EGP) from looking like attributes of every key.
ENTITY_MAX_ERROR_RATIO = 0.2
# In an attribute that nearly identifies its key (email, phone), a value
# held by at least this many keys, and this many times the usual number,
# is reported as shared.
SHARED_VALUE_MIN_KEYS = 10
SHARED_VALUE_FACTOR = 10
# Only columns with this share of missing values (and the reverse) are
# checked for missingness that depends on a category.
MISSINGNESS_MIN_SHARE = 0.01
# A category "decides" missingness when at least this share of its rows
# is missing, or at most 1 - this share.
MISSINGNESS_CATEGORY_PURITY = 0.98
# Share of the missing values the categories must account for.
MISSINGNESS_MIN_COVERAGE = 0.90
MISSINGNESS_MAX_CATEGORIES = 30


# ============================================================
# Patterns and name hints
# ============================================================

ID_NAME_TOKENS = {"id", "uuid", "guid"}
DATE_NAME_TOKENS = {"date", "datetime", "timestamp", "dob"}
TIME_NAME_TOKENS = {"time"}
BOOLEAN_NAME_TOKENS = {"is", "has", "flag"}
EMAIL_NAME_TOKENS = {"email", "mail"}
PHONE_NAME_TOKENS = {"phone", "mobile", "tel", "telephone"}
CURRENCY_NAME_TOKENS = {"currency", "ccy"}

BOOLEAN_PAIRS = (
    {"true", "false"},
    {"yes", "no"},
    {"y", "n"},
    {"t", "f"},
    {"0", "1"},
)

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_PATTERN = r"^\+?[\d\s().\-]{7,20}$"
CURRENCY_CODE_PATTERN = r"^[A-Z]{3}$"
# ISO 4217 codes that mean "no currency" or "test".
PLACEHOLDER_CURRENCY_CODES = {"XXX", "XTS"}

# hh:mm, hh:mm:ss, hh.mm.ss, optional fraction and am/pm.
TIME_PATTERN = (
    r"^(\d{1,2})[:.](\d{2})(?:[:.](\d{2})(?:\.\d+)?)?(?:\s?([ap]\.?m\.?))?$"
)
LEADING_ZERO_PATTERN = r"^[+-]?0\d+$"

# Date layouts read with an explicit format (fast and unambiguous).
ISO_DATE_PATTERN = r"^\d{4}-\d{2}(?:-\d{2})?(?:[T\s]\d{2}:\d{2}.*)?$"
COMPACT_DATE_PATTERN = r"^(?:19|20)\d{6}$"
# 7/31/2023, 16-08-2025, 3.4.25: day/month order decided from the data.
NUMERIC_DATE_PATTERN = r"^(\d{1,2})([/.\-])(\d{1,2})\2(\d{4}|\d{2})$"
# 2024/05/01, 2024.5.1 (year first, not ISO): order decided the same way.
YEAR_FIRST_DATE_PATTERN = r"^(\d{4})([/.])(\d{1,2})\2(\d{1,2})$"

# A number inside text, e.g. 'EGP 12,284.15' or '3780.11 LE'.
NUMBER_IN_TEXT_PATTERN = r"[+-]?\d[\d,.\s]*\d|[+-]?\d"


# ============================================================
# Helpers
# ============================================================

def safe_value(value: Any) -> Any:
    """Convert pandas/numpy values into JSON-safe values."""

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return str(value)

    if hasattr(value, "item"):
        value = value.item()

    # inf / -inf are not valid JSON.
    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def as_flags(mask: pd.Series) -> pd.Series:
    """Turn a nullable boolean result into plain True/False."""

    return mask.fillna(False).astype(bool)


def name_tokens(column: str) -> list[str]:
    """Split a column name into lowercase words.

    'customerID' -> ['customer', 'id'], 'Account-Balance' -> ['account',
    'balance']. Matching whole words avoids substring mistakes such as
    'count' inside 'account'.
    """

    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column.strip())
    return [
        token
        for token in re.split(r"[^A-Za-z0-9]+", snake.lower())
        if token
    ]


def missing_masks(
    series: pd.Series,
    null_tokens: frozenset[str],
) -> dict[str, pd.Series]:
    """Classify each value as true null, blank, null token, or present."""

    stripped = series.str.strip()

    null = series.isna().astype(bool)
    blank = (~null) & as_flags(stripped.eq(""))
    token = (~null) & as_flags(stripped.str.casefold().isin(null_tokens))

    return {
        "null": null,
        "blank": blank,
        "token": token,
        "missing": null | blank | token,
    }


def normalize_text(values: pd.Series) -> pd.Series:
    """Fold case, spacing and separators: 'Internet_Banking ' ->
    'internet banking'."""

    return (
        values.str.strip()
        .str.casefold()
        .str.replace(r"[\s_\-]+", " ", regex=True)
    )


def to_numeric_clean(values: pd.Series) -> pd.Series:
    """Parse values as numbers; unparseable and infinite values -> NaN."""

    numeric = pd.to_numeric(
        values.astype(object),
        errors="coerce",
    ).astype("float64")

    return numeric.where(numeric.abs() != float("inf"))


def recover_numbers(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Read numbers written with currency text or other separators.

    '3780.11 LE' -> 3780.11, 'EGP 12,284.15' -> 12284.15,
    '1786,35' -> 1786.35. Returns the numbers and the text around them.
    """

    text = values.str.strip()
    number = (
        text.str.extract(f"({NUMBER_IN_TEXT_PATTERN})", expand=False)
        .str.replace(r"\s", "", regex=True)
    )
    surrounding = (
        text.str.replace(NUMBER_IN_TEXT_PATTERN, "", regex=True).str.strip()
    )

    has_comma = as_flags(number.str.contains(",", regex=False))
    has_dot = as_flags(number.str.contains(".", regex=False))

    # '1786,35' or '1.234,56': the comma is the decimal mark.
    decimal_comma = (
        has_comma & ~has_dot
        & as_flags(number.str.match(r"^[+-]?\d+,\d{1,2}$"))
    ) | (
        has_comma & has_dot
        & as_flags(number.str.rfind(",").gt(number.str.rfind(".")))
    )

    cleaned = number.where(
        ~decimal_comma,
        number.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False),
    )
    cleaned = cleaned.where(
        decimal_comma,
        cleaned.str.replace(",", "", regex=False),
    )

    return to_numeric_clean(cleaned), surrounding


def _to_datetime(values: pd.Series, **kwargs: Any) -> pd.Series:
    """pd.to_datetime with coercion, as naive timestamps."""

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        parsed = pd.to_datetime(
            values.astype(object),
            errors="coerce",
            utc=True,
            **kwargs,
        )

    return parsed.dt.tz_localize(None).dt.as_unit("us")


def _read_numeric_dates(
    parts: pd.DataFrame,
    year_first: bool,
) -> tuple[pd.Series, list[dict[str, Any]]]:
    """Read dates split into year, separator and the two day/month parts.

    The day/month order is decided per separator from values that can
    only be read one way (a part above 12); ties are read month first.
    """

    first = to_numeric_clean(parts["first"])
    second = to_numeric_clean(parts["second"])
    year = to_numeric_clean(parts["year"])
    # Two-digit years: 00-69 -> 2000s, 70-99 -> 1900s.
    century = (year.lt(70) * 2000 + year.ge(70) * 1900).where(
        year.lt(100), 0,
    )
    year = year + century

    day_first = pd.Series(False, index=parts.index)
    orders: list[dict[str, Any]] = []

    for separator in sorted(parts["separator"].dropna().unique()):
        in_group = as_flags(parts["separator"].eq(separator))
        a, b = first[in_group], second[in_group]

        day_first_evidence = int(a.gt(12).sum())
        month_first_evidence = int(b.gt(12).sum())
        is_day_first = day_first_evidence > month_first_evidence
        order = "day/month" if is_day_first else "month/day"

        day_first[in_group] = is_day_first
        orders.append({
            "separator": separator,
            "layout": f"yyyy{separator}nn{separator}nn" if year_first
            else f"nn{separator}nn{separator}yyyy",
            "read_as": f"year/{order}" if year_first else f"{order}/year",
            "day_first": is_day_first,
            "day_first_evidence": day_first_evidence,
            "month_first_evidence": month_first_evidence,
            "ambiguous_count": int(
                (a.le(12) & b.le(12) & a.ne(b)).sum()
            ),
        })

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dates = pd.to_datetime(
            pd.DataFrame({
                "year": year,
                "month": second.where(day_first, first),
                "day": first.where(day_first, second),
            }),
            errors="coerce",
        )

    return dates.dt.as_unit("us"), orders


def parse_dates(
    values: pd.Series,
) -> tuple[pd.Series, list[dict[str, Any]]]:
    """Parse values as dates; unparseable values -> NaT.

    Known layouts are read with explicit formats. For d/m/y and y/m/d
    style values the day/month order is decided per layout and separator
    from values that can only be read one way (a part above 12). Anything
    else (07 Sep 2023, 12-Aug-24) falls back to pandas' mixed-format
    parser.

    Returns the dates and, per layout and separator, how the order was
    decided.
    """

    text = values.astype("string").str.strip()
    result = pd.Series(pd.NaT, index=text.index, dtype="datetime64[us]")
    orders: list[dict[str, Any]] = []

    # ---- ISO: 2025-08-16, 2025-08, 2025-08-16 10:00:00 ----
    iso = as_flags(text.str.match(ISO_DATE_PATTERN))
    if iso.any():
        result[iso] = _to_datetime(text[iso], format="ISO8601")

    # ---- Compact: 20251124 ----
    compact = as_flags(text.str.match(COMPACT_DATE_PATTERN))
    if compact.any():
        result[compact] = _to_datetime(text[compact], format="%Y%m%d")

    handled = iso | compact

    # ---- Numeric d/m/y or m/d/y, then y/m/d or y/d/m ----
    layouts = (
        (NUMERIC_DATE_PATTERN, ["first", "separator", "second", "year"],
         False),
        (YEAR_FIRST_DATE_PATTERN, ["year", "separator", "first", "second"],
         True),
    )

    for pattern, names, year_first in layouts:
        parts = text[~handled].str.extract(pattern)
        parts.columns = names
        parts = parts[parts["year"].notna()]

        if len(parts):
            dates, layout_orders = _read_numeric_dates(parts, year_first)
            result[parts.index] = dates
            orders.extend(layout_orders)
            handled = handled | text.index.isin(parts.index)

    # ---- Anything else: 07 Sep 2023, 12-Aug-24, 'Aug 12, 2024', ... ----
    rest = ~handled & as_flags(text.ne(""))
    if rest.any():
        result[rest] = _to_datetime(text[rest], format="mixed")

    return result, orders


def parse_times(values: pd.Series) -> pd.DataFrame:
    """Split times into parts and read them as seconds after midnight.

    Columns: hour, minute, second, has_am_pm, parsed, out_of_range and
    seconds ('07:19:20 PM' and '19:19:20' give the same seconds; NaN when
    unparseable or out of range).
    """

    parts = values.str.strip().str.casefold().str.extract(TIME_PATTERN)

    hour = to_numeric_clean(parts[0])
    minute = to_numeric_clean(parts[1])
    second = to_numeric_clean(parts[2])
    am_pm = parts[3].str.replace(".", "", regex=False)
    has_am_pm = am_pm.notna()

    parsed = hour.notna()
    out_of_range = parsed & (
        hour.gt(23)
        | (has_am_pm & (hour.gt(12) | hour.eq(0)))
        | minute.gt(59)
        | second.gt(59)
    )

    hour_24 = hour.where(
        ~has_am_pm, hour % 12 + as_flags(am_pm.eq("pm")) * 12,
    )
    seconds = (hour_24 * 3600 + minute * 60 + second.fillna(0)).where(
        parsed & ~out_of_range
    )

    return pd.DataFrame({
        "hour": hour,
        "minute": minute,
        "second": second,
        "has_am_pm": has_am_pm,
        "parsed": parsed,
        "out_of_range": out_of_range,
        "seconds": seconds,
    })


def make_note(code: str, detail: str) -> dict[str, str]:
    """A neutral observation about the data, not a rule result."""

    return {"code": code, "detail": detail}


def match_rate(values: pd.Series, pattern: str) -> float:
    """Share of values matching a regular expression."""

    if not len(values):
        return 0.0

    return float(as_flags(values.str.match(pattern)).mean())


def value_shape(values: pd.Series, collapse: bool) -> pd.Series:
    """Describe the layout of each value.

    Digits become 9, upper-case letters A, lower-case letters a. With
    collapse, runs are merged: '7/31/2023' -> '9/9/9'. Without it the
    length is kept: 'CUST043479' -> 'AAAA999999'.
    """

    plus = "+" if collapse else ""

    return (
        values.str.replace(rf"\d{plus}", "9", regex=True)
        .str.replace(rf"[A-Z]{plus}", "A", regex=True)
        .str.replace(rf"[a-z]{plus}", "a", regex=True)
    )


def format_patterns(
    values: pd.Series,
    collapse: bool,
) -> tuple[list[dict[str, Any]], int]:
    """Most common value layouts with a count and an example each.

    Returns the top patterns and the number of distinct patterns.
    """

    if not len(values):
        return [], 0

    shapes = value_shape(values, collapse)
    counts = shapes.value_counts()
    examples = values.groupby(shapes, sort=False).first()

    patterns = [
        {
            "pattern": pattern,
            "count": int(count),
            "example": safe_value(examples[pattern]),
        }
        for pattern, count in counts.head(TOP_N).items()
    ]

    return patterns, len(counts)


def pattern_note(
    patterns: list[dict[str, Any]],
    value_count: int,
) -> dict[str, str] | None:
    """Note for values that do not follow the most common layout."""

    if len(patterns) < 2:
        return None

    main = patterns[0]
    others = ", ".join(f"'{p['example']}'" for p in patterns[1:4])

    return make_note(
        "FORMAT_VARIANTS",
        f"{value_count - main['count']} values do not follow the main "
        f"pattern '{main['pattern']}' (e.g. '{main['example']}'); "
        f"other forms: {others}.",
    )


def top_counts(values: pd.Series) -> list[dict[str, Any]]:
    """Most frequent values with their counts."""

    return [
        {"value": safe_value(value), "count": int(count)}
        for value, count in values.value_counts().head(TOP_N).items()
    ]


def counts_text(values: pd.Series, limit: int = 5) -> str:
    """"'a' x3, 'b' x1" for the most frequent values."""

    return ", ".join(
        f"'{value}' x{count}"
        for value, count in values.value_counts().head(limit).items()
    )


# ============================================================
# Type inference (from the data; the name is only a hint)
# ============================================================

def infer_logical_type(
    column: str,
    values: pd.Series,
) -> dict[str, Any]:
    """Infer a logical type from the non-missing values of a column.

    Returns a dict with 'type' and 'basis', plus the parsed 'numeric' or
    'dates' series when relevant so they are not computed twice.
    """

    tokens = name_tokens(column)
    token_set = set(tokens)
    value_count = len(values)

    if value_count == 0:
        return {"type": "empty", "basis": "values"}

    # ---- Identifier: never treated as a number ----
    if tokens and (tokens[-1] in ID_NAME_TOKENS or tokens[0] == "id"):
        return {"type": "identifier", "basis": "name"}

    # ---- Phone: digits only, but not a quantity ----
    if token_set & PHONE_NAME_TOKENS:
        return {"type": "phone", "basis": "name"}

    stripped = values.str.strip()
    folded = stripped.str.casefold()

    # ---- Currency code: text, even when badly written ----
    if (
        token_set & CURRENCY_NAME_TOKENS
        and match_rate(stripped, CURRENCY_CODE_PATTERN) >= NAME_HINT_THRESHOLD
    ):
        return {"type": "currency_code", "basis": "name_and_values"}

    # ---- Boolean ----
    distinct = set(folded.unique().tolist())

    for pair in BOOLEAN_PAIRS:
        if distinct <= pair:
            # A column containing only "0" or only "1" is ambiguous,
            # so it needs a name hint to be called boolean.
            if (
                pair != {"0", "1"}
                or len(distinct) == 2
                or token_set & BOOLEAN_NAME_TOKENS
            ):
                return {"type": "boolean", "basis": "values"}

    # ---- Numeric ----
    numeric = to_numeric_clean(values)

    if float(numeric.notna().mean()) >= TYPE_THRESHOLD:
        valid = numeric.dropna()
        kind = "integer" if bool((valid % 1 == 0).all()) else "float"

        return {"type": kind, "basis": "values", "numeric": numeric}

    # ---- Time of day ----
    time_hint = bool(token_set & TIME_NAME_TOKENS)
    threshold = NAME_HINT_THRESHOLD if time_hint else TYPE_THRESHOLD

    if match_rate(folded, TIME_PATTERN) >= threshold:
        return {
            "type": "time",
            "basis": "name_and_values" if time_hint else "values",
        }

    # ---- Date / datetime ----
    date_hint = bool(token_set & DATE_NAME_TOKENS)
    threshold = NAME_HINT_THRESHOLD if date_hint else TYPE_THRESHOLD

    # Words like "may" or "sat" parse as dates, so require a digit.
    has_digit = as_flags(values.str.contains(r"\d", regex=True))
    candidates = values[has_digit]

    if len(candidates) / value_count >= threshold:
        # Cheap check on a sample before parsing a large column.
        sample = candidates.sample(
            min(len(candidates), DATE_SAMPLE_SIZE),
            random_state=0,
        )

        if float(parse_dates(sample)[0].notna().mean()) >= 0.5:
            dates, orders = parse_dates(candidates)
            dates = dates.reindex(values.index)

            if float(dates.notna().mean()) >= threshold:
                valid_dates = dates.dropna()
                has_time = bool(
                    (valid_dates != valid_dates.dt.normalize()).any()
                )

                return {
                    "type": "datetime" if has_time else "date",
                    "basis": "name_and_values" if date_hint else "values",
                    "dates": dates,
                    "date_orders": orders,
                }

    # ---- Email ----
    if match_rate(stripped, EMAIL_PATTERN) >= TYPE_THRESHOLD:
        return {"type": "email", "basis": "values"}

    if token_set & EMAIL_NAME_TOKENS:
        return {"type": "email", "basis": "name"}

    # ---- Categorical vs free text ----
    unique_count = int(values.nunique())

    if (
        unique_count <= CATEGORICAL_MAX_UNIQUE
        or unique_count / value_count <= CATEGORICAL_MAX_UNIQUE_RATIO
    ):
        return {"type": "categorical", "basis": "values"}

    return {"type": "text", "basis": "values"}


# ============================================================
# Column profiling
# ============================================================

def profile_numeric(
    profile: dict[str, Any],
    flags: list[dict[str, str]],
    values: pd.Series,
    stripped: pd.Series,
    numeric: pd.Series,
    logical_type: str,
) -> None:
    """Statistics, outliers and number-format problems."""

    value_count = len(values)
    valid = numeric.dropna()
    failures = values[numeric.isna()]
    failure_count = len(failures)

    def as_number(value: Any) -> Any:
        value = safe_value(value)
        if value is not None and logical_type == "integer":
            return int(value)
        return value

    leading_zero_count = int(
        as_flags(stripped.str.match(LEADING_ZERO_PATTERN)).sum()
    )

    quantiles = valid.quantile([0.01, 0.05, 0.25, 0.75, 0.95, 0.99])
    q1, q3 = quantiles.get(0.25), quantiles.get(0.75)
    far_outlier_count = 0
    repeated_extremes: list[dict[str, Any]] = []

    # With no spread between the quartiles every other value would count.
    if len(valid) and q3 > q1:
        spread = FAR_OUTLIER_IQR * (q3 - q1)
        far = valid.lt(q1 - spread) | valid.gt(q3 + spread)
        far_outlier_count = int(far.sum())

        # The same far-off min/max on many rows looks like a stand-in
        # for "unknown" (credit_score 0) rather than a real measurement.
        for extreme in (valid.min(), valid.max()):
            count = int(valid.eq(extreme).sum())
            if (
                (extreme < q1 - spread or extreme > q3 + spread)
                and count >= 2
                and count >= REPEATED_EXTREME_MIN_SHARE * len(valid)
            ):
                repeated_extremes.append(
                    {"value": as_number(extreme), "count": count}
                )

    profile.update({
        "numeric_parse_success_rate": round(
            len(valid) / value_count * 100, 4
        ),
        "numeric_parse_failure_count": failure_count,
        "numeric_parse_failures_sample": [
            safe_value(value) for value in failures.head(5).tolist()
        ],
        "min": as_number(valid.min()),
        "max": as_number(valid.max()),
        "mean": safe_value(valid.mean()),
        "median": safe_value(valid.median()),
        "std": safe_value(valid.std()),
        "percentiles": {
            f"p{round(q * 100):02d}": safe_value(value)
            for q, value in quantiles.items()
        },
        "far_outlier_count": far_outlier_count,
        "repeated_extreme_values": repeated_extremes,
        "count_at_min": int(valid.eq(valid.min()).sum()),
        "count_at_max": int(valid.eq(valid.max()).sum()),
        "zero_count": int(valid.eq(0).sum()),
        "negative_count": int(valid.lt(0).sum()),
        "leading_zero_count": leading_zero_count,
    })

    if failure_count:
        recovered, surrounding = recover_numbers(failures)
        recoverable = recovered.notna()
        patterns, _ = format_patterns(failures, collapse=True)
        extra_text = surrounding[recoverable & as_flags(surrounding.ne(""))]

        profile["numeric_failure_patterns"] = patterns
        profile["numeric_recoverable_count"] = int(recoverable.sum())
        profile["numeric_embedded_text"] = top_counts(extra_text)

        flags.append(make_note(
            "MIXED_TYPES",
            f"{failure_count} non-numeric values in a numeric column.",
        ))

        if recoverable.any():
            detail = (
                f"{int(recoverable.sum())} of them are numbers written "
                "with extra text or separators"
            )
            if len(extra_text):
                detail += f" (text found: {counts_text(extra_text)})"

            flags.append(make_note("NUMBER_FORMAT_VARIANTS", detail + "."))

    if far_outlier_count:
        flags.append(make_note(
            "FAR_OUTLIERS",
            f"{far_outlier_count} values lie more than {FAR_OUTLIER_IQR:g} "
            "IQRs outside the quartiles.",
        ))

    for extreme in repeated_extremes:
        flags.append(make_note(
            "REPEATED_EXTREME_VALUE",
            f"{extreme['value']} appears {extreme['count']} times far "
            "outside the other values; it may be a cap, a default or a "
            "stand-in for an unknown value.",
        ))

    if leading_zero_count:
        flags.append(make_note(
            "LEADING_ZEROS",
            f"{leading_zero_count} values start with 0; this may be "
            "a code or identifier rather than a number.",
        ))


def profile_dates(
    profile: dict[str, Any],
    flags: list[dict[str, str]],
    values: pd.Series,
    dates: pd.Series,
    orders: list[dict[str, Any]],
) -> None:
    """Range, parse failures, formats and day/month order."""

    value_count = len(values)
    valid_dates = dates.dropna()
    failures = values[dates.isna()]
    failure_count = len(failures)
    # Lengths are kept so 2022-12-19 and 19-12-2022 stay apart.
    patterns, pattern_count = format_patterns(values, collapse=False)

    now = pd.Timestamp.now()
    future_count = int(valid_dates.gt(now).sum())
    before_1900_count = int(valid_dates.lt(pd.Timestamp("1900-01-01")).sum())

    profile.update({
        "date_parse_success_count": len(valid_dates),
        "date_parse_failure_count": failure_count,
        "date_parse_failures_sample": [
            safe_value(value) for value in failures.head(5).tolist()
        ],
        "min_date": safe_value(valid_dates.min()),
        "max_date": safe_value(valid_dates.max()),
        "future_date_count": future_count,
        "before_1900_count": before_1900_count,
        "date_format_pattern_count": pattern_count,
        "date_format_patterns": patterns,
        "day_month_order": orders,
    })

    if failure_count:
        flags.append(make_note(
            "MIXED_TYPES",
            f"{failure_count} values could not be parsed as dates: "
            + counts_text(failures),
        ))

    note = pattern_note(patterns, value_count)
    if note:
        flags.append(note)

    for order in orders:
        layout = order["layout"]
        both = order["day_first_evidence"] and order["month_first_evidence"]

        if both:
            flags.append(make_note(
                "AMBIGUOUS_DATES",
                f"Dates laid out as '{layout}' are written both day-first "
                f"({order['day_first_evidence']} values) and month-first "
                f"({order['month_first_evidence']} values); "
                f"{order['ambiguous_count']} values could be either and "
                f"were read as {order['read_as']}.",
            ))
        elif order["ambiguous_count"] and not (
            order["day_first_evidence"] or order["month_first_evidence"]
        ):
            flags.append(make_note(
                "AMBIGUOUS_DATES",
                f"The day/month order of dates laid out as '{layout}' "
                f"cannot be worked out; {order['ambiguous_count']} values "
                f"were read as {order['read_as']}.",
            ))

    # Day-first next to month-first among year-last dates is the risky
    # mix; year-first dates (2024/05/01) are normally year/month/day.
    year_last = [o for o in orders if not o["layout"].startswith("yyyy")]
    year_first_unusual = [
        o for o in orders
        if o["layout"].startswith("yyyy") and o["day_first"]
    ]

    if (
        len({order["day_first"] for order in year_last}) > 1
        or year_first_unusual
    ):
        flags.append(make_note(
            "MIXED_DATE_CONVENTIONS",
            "; ".join(
                f"'{order['layout']}' dates read as {order['read_as']}"
                for order in orders
            ) + ".",
        ))

    if future_count:
        flags.append(make_note(
            "FUTURE_DATES",
            f"{future_count} dates are later than the profiling time.",
        ))

    if before_1900_count:
        flags.append(make_note(
            "VERY_OLD_DATES",
            f"{before_1900_count} dates are before 1900.",
        ))


def profile_times(
    profile: dict[str, Any],
    flags: list[dict[str, str]],
    values: pd.Series,
) -> None:
    """Parse failures, formats and out-of-range times (e.g. 25:61:00)."""

    value_count = len(values)
    folded = values.str.strip().str.casefold()
    times = parse_times(values)
    parsed = times["parsed"]
    out_of_range = times["out_of_range"]

    failures = values[~parsed]
    invalid = values[out_of_range]
    patterns, pattern_count = format_patterns(values, collapse=True)

    profile.update({
        "time_parse_failure_count": len(failures),
        "time_parse_failures_sample": [
            safe_value(value) for value in failures.head(5).tolist()
        ],
        "out_of_range_time_count": len(invalid),
        "out_of_range_time_top_values": top_counts(invalid),
        "time_format_pattern_count": pattern_count,
        "time_format_patterns": patterns,
    })

    if len(failures):
        flags.append(make_note(
            "MIXED_TYPES",
            f"{len(failures)} values could not be parsed as times.",
        ))

    if len(invalid):
        detail = (
            f"{len(invalid)} times are out of range: {counts_text(invalid)}."
        )
        if as_flags(folded[out_of_range].str.match(r"^24[:.]00")).any():
            detail += " 24:00 is sometimes used for end of day."

        flags.append(make_note("OUT_OF_RANGE_TIMES", detail))

    note = pattern_note(patterns, value_count)
    if note:
        flags.append(note)


def profile_format(
    profile: dict[str, Any],
    flags: list[dict[str, str]],
    values: pd.Series,
    stripped: pd.Series,
    logical_type: str,
) -> None:
    """Values that do not look like a valid email, phone or currency."""

    if logical_type == "email":
        valid_format = as_flags(stripped.str.match(EMAIL_PATTERN))
    elif logical_type == "currency_code":
        valid_format = as_flags(stripped.str.match(CURRENCY_CODE_PATTERN))
    else:
        valid_format = (
            as_flags(stripped.str.match(PHONE_PATTERN))
            & as_flags(stripped.str.count(r"\d").ge(7))
        )

    invalid = values[~valid_format]

    profile["invalid_format_count"] = len(invalid)
    profile["invalid_format_top_values"] = top_counts(invalid)

    if len(invalid):
        label = {
            "email": "email addresses",
            "phone": "phone numbers",
            "currency_code": "ISO 4217 currency codes",
        }[logical_type]

        flags.append(make_note(
            "INVALID_FORMAT",
            f"{len(invalid)} values are not valid {label}: "
            + counts_text(invalid),
        ))

    if logical_type == "currency_code":
        placeholders = values[as_flags(stripped.isin(PLACEHOLDER_CURRENCY_CODES))]

        if len(placeholders):
            flags.append(make_note(
                "PLACEHOLDER_CODES",
                f"{len(placeholders)} values are ISO placeholder codes "
                f"meaning 'no currency': {counts_text(placeholders)}.",
            ))

    if logical_type == "phone":
        patterns, pattern_count = format_patterns(values, collapse=False)
        profile["phone_format_pattern_count"] = pattern_count
        profile["phone_format_patterns"] = patterns

        note = pattern_note(patterns, len(values))
        if note:
            flags.append(note)


def canonical_values(
    values: pd.Series,
    stripped: pd.Series,
    inferred: dict[str, Any],
) -> pd.Series:
    """The non-missing values in one comparable form.

    Used by the cross-column checks so formatting noise is not mistaken
    for a different value: '  MALE ', 'Male_' and 'male' -> 'male';
    '07 Sep 2023' and '2023-09-07' -> the same date; '07:19:20 PM' and
    '19:19:20' -> the same time. Values that cannot be read keep their
    stripped text.
    """

    logical_type = inferred["type"]

    if logical_type in ("categorical", "text", "boolean", "currency_code"):
        distinct = stripped.dropna().unique()
        mapping = dict(zip(
            distinct, normalize_text(pd.Series(distinct, dtype="string")),
        ))
        return stripped.map(mapping)

    if logical_type == "email":
        return stripped.str.casefold()

    if logical_type in ("integer", "float"):
        parsed = inferred["numeric"]
    elif logical_type in ("date", "datetime"):
        parsed = inferred["dates"]
    elif logical_type == "time":
        parsed = parse_times(values)["seconds"]
    else:
        return stripped

    readable = parsed.notna()
    if readable.all():
        return parsed

    canonical = stripped.astype(object)
    canonical[readable] = parsed[readable]
    return canonical


def profile_column(
    series: pd.Series,
    masks: dict[str, pd.Series],
) -> tuple[dict[str, Any], pd.Series]:
    """Profile a single column without modifying the source data.

    Returns the profile and the canonical non-missing values (see
    canonical_values) for the cross-column checks.
    """

    column = str(series.name)
    values = series[~masks["missing"]]
    stripped = values.str.strip()

    row_count = len(series)
    value_count = len(values)
    missing_count = int(masks["missing"].sum())
    null_token_count = int(masks["token"].sum())
    unique_count = int(values.nunique())
    whitespace_count = int(as_flags(values.ne(stripped)).sum())

    missing_percentage = (
        round(missing_count / row_count * 100, 4) if row_count else 0
    )

    inferred = infer_logical_type(column, values)
    logical_type = inferred["type"]

    profile: dict[str, Any] = {
        "column_name": column,
        "logical_type": logical_type,
        "type_inference_basis": inferred["basis"],
        "row_count": row_count,
        "null_count": int(masks["null"].sum()),
        "blank_count": int(masks["blank"].sum()),
        "null_token_count": null_token_count,
        "missing_count": missing_count,
        "missing_percentage": missing_percentage,
        "unique_count": unique_count,
        "unique_percentage": round(
            unique_count / value_count * 100, 4
        ) if value_count else 0,
        "whitespace_count": whitespace_count,
        "sample_values": [
            safe_value(value) for value in values.head(5).tolist()
        ],
    }

    # Neutral notes on things worth a look when writing the rules.
    flags: list[dict[str, str]] = []

    # --------------------------------------------------------
    # Missing-value observations
    # --------------------------------------------------------

    if row_count and value_count == 0:
        flags.append(make_note(
            "ALL_MISSING", "Column has no usable values.",
        ))

    if null_token_count:
        tokens_found = series[masks["token"]].str.strip()
        flags.append(make_note(
            "NULL_TOKENS",
            f"{null_token_count} values are null placeholders: "
            + counts_text(tokens_found),
        ))

    if whitespace_count:
        flags.append(make_note(
            "WHITESPACE",
            f"{whitespace_count} values have leading/trailing spaces.",
        ))

    if unique_count == 1 and row_count > 1:
        flags.append(make_note(
            "CONSTANT", "Column contains a single distinct value.",
        ))

    # --------------------------------------------------------
    # Type-specific profiling
    # --------------------------------------------------------

    if logical_type in ("integer", "float"):
        profile_numeric(
            profile, flags, values, stripped, inferred["numeric"],
            logical_type,
        )

    if logical_type in ("date", "datetime"):
        profile_dates(
            profile, flags, values, inferred["dates"],
            inferred["date_orders"],
        )

    if logical_type == "time":
        profile_times(profile, flags, values)

    if logical_type in ("email", "phone", "currency_code"):
        profile_format(profile, flags, values, stripped, logical_type)

    if logical_type == "identifier":
        # Repeats beyond the first occurrence of each value.
        profile["duplicate_value_count"] = int(values.duplicated().sum())

        patterns, pattern_count = format_patterns(values, collapse=False)
        profile["id_format_pattern_count"] = pattern_count
        profile["id_format_patterns"] = patterns

        note = pattern_note(patterns, value_count)
        if note:
            flags.append(note)

    # --------------------------------------------------------
    # Consistency profiling
    # --------------------------------------------------------

    if value_count:
        counts = values.value_counts()
        distinct = pd.Series(counts.index, index=counts.index).astype("string")
        normalized = normalize_text(distinct)
        group_sizes = normalized.value_counts()
        variant_keys = group_sizes[group_sizes.gt(1)].index

        # Same value apart from case, spacing or separators, e.g.
        # 'Mobile App', 'mobile app', 'MOBILE APP', 'Internet_Banking'.
        profile["spelling_variant_count"] = len(variant_keys)
        profile["spelling_variant_groups"] = [
            {
                "normalized": key,
                "variants": [
                    {"value": value, "count": int(counts[value])}
                    for value in distinct[normalized.eq(key)].index
                ],
            }
            for key in variant_keys[:TOP_N]
        ]

        profile["top_values"] = [
            {"value": safe_value(value), "count": int(count)}
            for value, count in counts.head(TOP_N).items()
        ]

        if len(variant_keys) and logical_type in (
            "categorical", "text", "currency_code",
        ):
            examples = "; ".join(
                " / ".join(f"'{v['value']}'" for v in group["variants"])
                for group in profile["spelling_variant_groups"][:3]
            )
            flags.append(make_note(
                "SPELLING_VARIANTS",
                f"{len(variant_keys)} values appear in more than one "
                f"spelling (case, spacing or separators): {examples}.",
            ))

        if logical_type in ("categorical", "text"):
            generic = counts[
                as_flags(normalized.isin(GENERIC_CATEGORY_VALUES)).to_numpy()
            ]
            profile["generic_value_count"] = int(generic.sum())

            if len(generic):
                found = ", ".join(
                    f"'{value}' x{count}"
                    for value, count in generic.head(5).items()
                )
                flags.append(make_note(
                    "GENERIC_VALUES",
                    f"{int(generic.sum())} values are catch-all labels "
                    f"rather than a specific category: {found}.",
                ))

    profile["observations"] = flags

    return profile, canonical_values(values, stripped, inferred)


# ============================================================
# Cross-column profiling
# ============================================================

def category_codes(
    canonical: dict[str, pd.Series],
    index: pd.Index,
) -> tuple[pd.DataFrame, dict[str, pd.Index]]:
    """Integer code per distinct canonical value (-1 when missing), for
    fast grouping. Also returns the value behind each code."""

    codes: dict[str, np.ndarray] = {}
    uniques: dict[str, pd.Index] = {}

    for column, values in canonical.items():
        column_codes, column_uniques = pd.factorize(values.reindex(index))
        codes[column] = column_codes.astype(np.int64)
        uniques[column] = pd.Index(column_uniques)

    return pd.DataFrame(codes, index=index), uniques


def key_value_stats(
    key_codes: np.ndarray,
    value_codes: np.ndarray,
) -> dict[str, Any] | None:
    """How consistently each key carries one value of another column.

    Only rows where both are present count. Returns None when there are
    no such rows.
    """

    both = (key_codes >= 0) & (value_codes >= 0)
    if not both.any():
        return None

    keys = key_codes[both]
    values = value_codes[both]
    width = int(values.max()) + 1

    pairs, pair_rows = np.unique(keys * width + values, return_counts=True)
    pair_keys = pairs // width
    pair_values = pairs % width

    # np.unique sorts, so each key's pairs are next to each other.
    starts = np.flatnonzero(np.r_[True, pair_keys[1:] != pair_keys[:-1]])
    values_per_key = np.diff(np.r_[starts, len(pairs)])
    rows_per_key = np.add.reduceat(pair_rows, starts)
    majority_rows = int(np.maximum.reduceat(pair_rows, starts).sum())

    return {
        "rows": len(keys),
        "keys": len(starts),
        # Keys seen on two or more rows: only these can show a conflict.
        "testable_keys": int((rows_per_key >= 2).sum()),
        "conflicting_keys": int((values_per_key > 1).sum()),
        # Rows that differ from their key's most common value.
        "conflicting_rows": len(keys) - majority_rows,
        "top_value_rows": int(np.bincount(values).max()),
        "pair_values": pair_values,
    }


def shared_values(
    stats: dict[str, Any],
    uniques: pd.Index,
) -> list[dict[str, Any]]:
    """Values of a near-identifying attribute held by unusually many keys.

    E.g. 'invalid-email' carried by thousands of customers, when each
    real email belongs to one. Empty when the column does not come close
    to identifying its key (names, cities).
    """

    keys_per_value = np.bincount(stats["pair_values"])
    used = keys_per_value[keys_per_value > 0]

    if len(used) < 0.5 * stats["keys"]:
        return []

    limit = max(
        SHARED_VALUE_MIN_KEYS,
        SHARED_VALUE_FACTOR * float(np.median(used)),
    )
    shared = np.flatnonzero(keys_per_value >= limit)
    shared = shared[np.argsort(-keys_per_value[shared])][:TOP_N]

    return [
        {
            "value": safe_value(uniques[code]),
            "key_count": int(keys_per_value[code]),
        }
        for code in shared
    ]


def key_relationships(
    codes: pd.DataFrame,
    uniques: dict[str, pd.Index],
    columns: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """For each repeated identifier, the columns that belong to it.

    In a denormalised extract a customer_id repeats on every transaction
    and should always carry the same name, email, date of birth, etc.
    Values are compared in canonical form, so 'Male' vs 'MALE' or
    2002-04-15 vs 15/04/2002 is not a conflict. A column counts as an
    attribute of the key when nearly all its rows agree with their key's
    most common value, and far more than they agree with the column's
    overall most common value.

    Identifiers that are almost unique (transaction_id) are not entities;
    their repeated values are reported as duplicate keys, with the
    columns in which the repeated rows differ.

    Returns (entity attributes, duplicate keys).
    """

    row_count = len(codes)
    entities: list[dict[str, Any]] = []
    duplicate_keys: list[dict[str, Any]] = []

    for profile in columns:
        if (
            profile["logical_type"] != "identifier"
            or not profile.get("duplicate_value_count")
        ):
            continue

        key = profile["column_name"]
        key_codes = codes[key].to_numpy()
        repeated = (key_codes >= 0) & pd.Series(key_codes).duplicated(
            keep=False
        ).to_numpy()
        repeated_keys = len(np.unique(key_codes[repeated]))

        if not repeated_keys:
            continue

        others = [column for column in codes.columns if column != key]

        # ---- Near key: describe the repeats, not an entity ----
        if profile["unique_count"] >= NEAR_KEY_MIN_SHARE * row_count:
            repeated_codes = np.where(repeated, key_codes, -1)
            differing: dict[str, int] = {}

            for column in others:
                stats = key_value_stats(
                    repeated_codes, codes[column].to_numpy(),
                )
                if stats and stats["conflicting_keys"]:
                    differing[column] = stats["conflicting_keys"]

            duplicate_keys.append({
                "key_column": key,
                "repeated_key_count": repeated_keys,
                "row_count": int(repeated.sum()),
                "differing_columns": dict(
                    sorted(differing.items(), key=lambda item: -item[1])
                ),
            })
            continue

        # ---- Entity key: find its attributes ----
        attributes: list[str] = []
        conflicting_keys: dict[str, int] = {}
        conflicting_rows: dict[str, int] = {}
        shared: dict[str, list[dict[str, Any]]] = {}

        for column in others:
            stats = key_value_stats(key_codes, codes[column].to_numpy())

            # Only keys with two or more values in a column say anything
            # about it; mostly empty columns would otherwise look
            # consistent.
            if not stats or stats["testable_keys"] < 0.5 * repeated_keys:
                continue

            error = stats["conflicting_rows"] / stats["rows"]
            baseline_error = 1 - stats["top_value_rows"] / stats["rows"]

            if (
                error > 1 - ENTITY_ATTRIBUTE_MIN_SHARE
                or baseline_error == 0
                or error > ENTITY_MAX_ERROR_RATIO * baseline_error
            ):
                continue

            attributes.append(column)

            if stats["conflicting_keys"]:
                conflicting_keys[column] = stats["conflicting_keys"]
                conflicting_rows[column] = stats["conflicting_rows"]

            values = shared_values(stats, uniques[column])
            if values:
                shared[column] = values

        entities.append({
            "key_column": key,
            "repeated_key_count": repeated_keys,
            "attribute_columns": attributes,
            "conflicting_key_counts": conflicting_keys,
            "conflicting_row_counts": conflicting_rows,
            "shared_values": shared,
        })

    return entities, duplicate_keys


def conditional_missingness(
    codes: pd.DataFrame,
    uniques: dict[str, pd.Index],
    masks: dict[str, dict[str, pd.Series]],
    columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Columns whose missing values depend on a category.

    E.g. merchant_name is only filled for purchases and bill payments,
    failure_reason only for failed transactions. For each partly missing
    column, finds the categorical column whose values (in canonical form,
    so 'Bill_Payment' counts as 'bill payment') best split the rows into
    "always missing" and "always filled".
    """

    row_count = len(codes)
    if not row_count:
        return []

    drivers = [
        profile["column_name"]
        for profile in columns
        if profile["logical_type"] in ("categorical", "boolean")
        and len(uniques[profile["column_name"]]) <= MISSINGNESS_MAX_CATEGORIES
    ]

    results: list[dict[str, Any]] = []

    for profile in columns:
        column = profile["column_name"]
        share = profile["missing_count"] / row_count

        if not MISSINGNESS_MIN_SHARE <= share <= 1 - MISSINGNESS_MIN_SHARE:
            continue

        missing = masks[column]["missing"]
        best: dict[str, Any] | None = None

        for driver in drivers:
            if driver == column:
                continue

            known = codes[driver].ge(0)
            stats = (
                missing[known]
                .groupby(codes[driver][known])
                .agg(["mean", "size", "sum"])
            )

            always_missing = stats["mean"].ge(MISSINGNESS_CATEGORY_PURITY)
            always_filled = stats["mean"].le(1 - MISSINGNESS_CATEGORY_PURITY)
            pure_share = (
                stats.loc[always_missing | always_filled, "size"].sum()
                / stats["size"].sum()
            )
            coverage = stats.loc[always_missing, "sum"].sum() / missing.sum()

            if (
                pure_share < MISSINGNESS_CATEGORY_PURITY
                or coverage < MISSINGNESS_MIN_COVERAGE
                or (best and coverage <= best["coverage"])
            ):
                continue

            exceptions = int(
                (stats.loc[always_missing, "size"]
                 - stats.loc[always_missing, "sum"]).sum()
                + stats.loc[~always_missing, "sum"].sum()
            )

            best = {
                "column": column,
                "explained_by": driver,
                "coverage": coverage,
                "missing_when": [
                    safe_value(uniques[driver][int(code)])
                    for code in stats[always_missing]
                    .sort_values("size", ascending=False).index
                ],
                "exception_count": exceptions,
            }

        if best:
            best["missing_explained_percentage"] = round(
                best.pop("coverage") * 100, 4
            )
            results.append(best)

    return results


# ============================================================
# File profiling
# ============================================================

def read_csv(file_path: Path, delimiter: str) -> tuple[pd.DataFrame, str]:
    """Read a CSV as strings, trying common encodings."""

    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            # Read as strings to preserve original source values.
            df = pd.read_csv(
                file_path,
                dtype="string",
                keep_default_na=False,
                sep=delimiter,
                encoding=encoding,
            )
            return df, encoding
        except UnicodeDecodeError:
            continue

    raise ValueError(f"Could not decode {file_path}")


def profile_file(
    file_path: Path,
    base_dir: Path,
    dataset_name: str,
    null_tokens: frozenset[str],
    delimiter: str,
    relationships: bool = True,
) -> dict[str, Any]:
    """Profile one CSV file."""

    print(f"\nProfiling: {file_path}")

    df, encoding = read_csv(file_path, delimiter)
    return profile_frame(
        df, dataset_name, null_tokens,
        source_file=file_path.name,
        source_relative_path=file_path.relative_to(base_dir).as_posix(),
        encoding=encoding,
        relationships=relationships,
    )


def profile_frame(
    df: pd.DataFrame,
    dataset_name: str,
    null_tokens: frozenset[str],
    source_file: str,
    source_relative_path: str,
    encoding: str,
    relationships: bool = True,
) -> dict[str, Any]:
    """Profile a table read as text (every column a string column)."""

    masks = {
        column: missing_masks(df[column], null_tokens)
        for column in df.columns
    }

    columns = []
    canonical: dict[str, pd.Series] = {}
    for column in df.columns:
        print(f"  column: {column}")
        profile, values = profile_column(df[column], masks[column])
        columns.append(profile)
        if relationships:
            canonical[column] = values

    duplicate_rows = int(df.duplicated().sum())

    file_flags: list[dict[str, str]] = []

    if duplicate_rows:
        file_flags.append(make_note(
            "DUPLICATE_ROWS",
            f"{duplicate_rows} rows are exact copies of an earlier row.",
        ))

    untidy_names = [c for c in df.columns if c != c.strip()]

    if untidy_names:
        file_flags.append(make_note(
            "COLUMN_NAME_WHITESPACE",
            f"Column names with surrounding spaces: {untidy_names}",
        ))

    # Columns that are fully populated and unique: possible keys.
    candidate_keys = [
        column["column_name"]
        for column in columns
        if len(df)
        and column["missing_count"] == 0
        and column["unique_count"] == len(df)
    ]

    # Identifiers that would be keys apart from missing or repeated values.
    for column in columns:
        name = column["column_name"]
        if (
            column["logical_type"] == "identifier"
            and name not in candidate_keys
            and column["unique_count"] >= NEAR_KEY_MIN_SHARE * len(df)
        ):
            file_flags.append(make_note(
                "NEAR_KEY",
                f"'{name}' is almost unique but has "
                f"{column['missing_count']} missing and "
                f"{column.get('duplicate_value_count', 0)} repeated values.",
            ))

    result: dict[str, Any] = {
        "dataset_name": dataset_name,
        "source_file": source_file,
        "source_relative_path": source_relative_path,
        "encoding": encoding,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_row_count": duplicate_rows,
        "duplicate_row_percentage": round(
            duplicate_rows / len(df) * 100, 4
        ) if len(df) else 0,
        "candidate_key_columns": candidate_keys,
        "observations": file_flags,
    }

    # --------------------------------------------------------
    # Cross-column relationships
    # --------------------------------------------------------

    if relationships and len(df):
        print("  cross-column relationships")

        codes, uniques = category_codes(canonical, df.index)
        del canonical

        entities, duplicate_keys = key_relationships(codes, uniques, columns)
        missingness = conditional_missingness(codes, uniques, masks, columns)

        for item in duplicate_keys:
            differing = ", ".join(
                f"{column} ({count})"
                for column, count in item["differing_columns"].items()
            ) or "no column"
            file_flags.append(make_note(
                "DUPLICATE_KEYS",
                f"{item['repeated_key_count']} '{item['key_column']}' values "
                f"appear on more than one row ({item['row_count']} rows); "
                f"the rows sharing a value differ in: {differing}.",
            ))

        for entity in entities:
            key = entity["key_column"]

            for column, count in entity["conflicting_key_counts"].items():
                file_flags.append(make_note(
                    "ENTITY_CONFLICTS",
                    f"{count} '{key}' values have more than one "
                    f"'{column}' ({entity['conflicting_row_counts'][column]}"
                    " rows differ from the key's usual value).",
                ))

            for column, values in entity["shared_values"].items():
                file_flags.append(make_note(
                    "SHARED_VALUES",
                    f"'{column}' values shared by many '{key}' values: "
                    + ", ".join(
                        f"'{item['value']}' ({item['key_count']} keys)"
                        for item in values[:5]
                    ) + ".",
                ))

        for item in missingness:
            file_flags.append(make_note(
                "CONDITIONAL_MISSING",
                f"'{item['column']}' is missing when '{item['explained_by']}'"
                f" is one of {item['missing_when']} "
                f"({item['exception_count']} rows do not follow this).",
            ))

        result["relationships"] = {
            "duplicate_keys": duplicate_keys,
            "entity_attributes": entities,
            "conditional_missingness": missingness,
        }

    result["columns"] = columns

    return result


# ============================================================
# Main
# ============================================================

def output_name_for(file_path: Path, base_dir: Path) -> str:
    """Build a collision-free output filename from the relative path."""

    relative_path = file_path.relative_to(base_dir)

    name = (
        str(relative_path.with_suffix(""))
        .replace("\\", "__")
        .replace("/", "__")
        + "_profile.json"
    )

    return re.sub(r'[<>:"|?*]', "_", name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile CSV files and write one JSON report per file.",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="CSV file, or folder searched recursively for CSV files.",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Folder where the JSON profiles are written.",
    )
    parser.add_argument(
        "--dataset-name", default=DEFAULT_DATASET_NAME,
        help="Label stored in every profile.",
    )
    parser.add_argument(
        "--null-tokens", nargs="*", default=list(DEFAULT_NULL_TOKENS),
        help="Text values treated as missing (case-insensitive).",
    )
    parser.add_argument(
        "--delimiter", default=",",
        help="CSV field separator.",
    )
    parser.add_argument(
        "--skip-relationships", action="store_true",
        help="Skip the cross-column checks (entity attributes and "
        "conditional missingness).",
    )
    parser.add_argument(
        "--ingestion", default=None,
        help="Ingestion id (or 'latest') of --dataset-name in the bronze "
        "layer: profiles its rows and stores the profile with the "
        "ingestion. Overrides --input and --output.",
    )

    return parser.parse_args()


def profile_ingestion(args: argparse.Namespace, null_tokens: frozenset[str]) -> int:
    """Profile an ingestion in bronze and store the profile with it."""

    with bronze.connect() as conn:
        ingestion = bronze.find_ingestion(conn, args.dataset_name, args.ingestion)
        print(f"\nProfiling: {ingestion['ingestion_id']} "
              f"({ingestion['source_file']})")

        profile = profile_frame(
            bronze.read_frame(conn, ingestion),
            ingestion["dataset"],
            null_tokens,
            source_file=ingestion["source_file"],
            source_relative_path=ingestion["source_path"],
            encoding=ingestion["encoding"],
            relationships=not args.skip_relationships,
        )
        profile["ingestion_id"] = ingestion["ingestion_id"]
        bronze.save_profile(conn, ingestion["ingestion_id"], profile)

    print(f"Stored: bronze.ingestion.profile of {ingestion['ingestion_id']}")
    return 0


def main() -> int:
    args = parse_args()

    if args.ingestion:
        null_tokens = frozenset(
            token.strip().casefold() for token in args.null_tokens
        ) - {""}
        return profile_ingestion(args, null_tokens)

    input_path: Path = args.input.resolve()
    output_dir: Path = args.output

    if input_path.is_file():
        files = [input_path]
        base_dir = input_path.parent
    elif input_path.is_dir():
        files = sorted(
            path for path in input_path.rglob("*")
            if path.is_file() and path.suffix.lower() == ".csv"
        )
        base_dir = input_path
    else:
        raise FileNotFoundError(f"Input not found: {input_path}")

    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_path}")

    null_tokens = frozenset(
        token.strip().casefold() for token in args.null_tokens
    ) - {""}

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(files)} CSV files.")

    failed: list[Path] = []

    for file_path in files:
        # One unreadable file should not stop the whole run.
        try:
            profile = profile_file(
                file_path,
                base_dir,
                args.dataset_name,
                null_tokens,
                args.delimiter,
                relationships=not args.skip_relationships,
            )
        except Exception as error:  # noqa: BLE001
            print(f"FAILED: {file_path} ({type(error).__name__}: {error})")
            failed.append(file_path)
            continue

        output_path = output_dir / output_name_for(file_path, base_dir)

        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(
                profile,
                output_file,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        print(f"Created: {output_path}")

    if failed:
        print(f"\nCompleted with {len(failed)} failed file(s).")
        return 1

    print("\nData profiling completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
