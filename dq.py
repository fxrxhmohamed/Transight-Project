"""One command for every data-quality step.

    python dq.py <step> [options]

Steps, in pipeline order
    ingest              load a source file into the bronze layer (PostgreSQL);
                        prints the new ingestion id as the last line
                        (Airflow XCom)
    profile             profile an ingestion (stored with it in bronze)
    validate            validate an ingestion: valid records, quarantine,
                        remediation audit
    export-template     correction template (CSV) for the open quarantine round
    import-corrections  import a filled-in template (--file)
    revalidate          apply the corrections and validate again
    merge               valid + resolved records -> silver.<dataset> (typed)
    metrics             the DQ metric views for Power BI (metrics schema,
                        every run): apply sql/04_metrics.sql and check them
    metadata            describe silver in the meta schema (metadata engine)
    gold                build the gold star schema from the metadata (AutoETL)
    import-files        load runs kept in data/ by earlier versions into
                        the database (once)
Helpers
    status              quarantine counts per status and round
    ui                  start the correction UI (Streamlit)
    pipeline            ingest, profile, validate, merge, metrics, metadata
                        and gold in one go

Every step takes --run (an ingestion id, default 'latest') and --rules,
and passes the options to the service script that does the work. The
data work runs on Spark (PySpark, local or SPARK_MASTER); every step reads
and writes PostgreSQL (sql/*.sql); the database comes from the POSTGRES_*
environment variables (see .env.example). Run it in the pipeline container
(docker compose run --rm pipeline python dq.py ...), which has Java:

    ingest              services/ingestion-service/src/ingest.py
    profile             services/metadata-service/src/profiler.py
    validate            services/dq-service/src/dq_engine.py
    export-template,
    import-corrections,
    revalidate, status  services/dq-service/src/quarantine.py
    merge               services/dq-service/src/merge.py
    metrics             services/dq-service/src/metrics.py
    metadata            services/metadata-service/src/metadata.py
    gold                services/etl-service/src/gold.py
    import-files        services/dq-service/src/import_files.py

Exit code: the step's own (0 = success).

Examples
    python dq.py ingest --input data/raw/banking/banking_transactions.csv
    python dq.py validate --run ING_20261011_001
    python dq.py import-corrections --run ING_20261011_001 --file fixes.csv
    python dq.py pipeline --input landing/today.csv
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parent
SERVICES = ROOT / "services"

INGEST = SERVICES / "ingestion-service" / "src" / "ingest.py"
PROFILER = SERVICES / "metadata-service" / "src" / "profiler.py"
METADATA = SERVICES / "metadata-service" / "src" / "metadata.py"
GOLD = SERVICES / "etl-service" / "src" / "gold.py"
DQ = SERVICES / "dq-service" / "src"
ENGINE = DQ / "dq_engine.py"
QUARANTINE = DQ / "quarantine.py"
MERGE = DQ / "merge.py"
METRICS = DQ / "metrics.py"
IMPORT_FILES = DQ / "import_files.py"
UI = DQ / "correction_ui.py"

DEFAULT_RULES = ROOT / "config" / "rules" / "quality_rules.yaml"
DEFAULT_DATA = ROOT / "data"


def load_env(path: Path) -> None:
    """KEY=value lines of .env, for settings not already in the environment
    (the same file docker compose reads, so both use one database)."""

    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.strip().startswith("#"):
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def dataset_name(rules: Path) -> str:
    """The dataset name from the schema registry named in the rules file."""

    config = yaml.safe_load(rules.read_text(encoding="utf-8"))
    schema = yaml.safe_load((ROOT / config["schema"]).read_text(encoding="utf-8"))
    return schema.get("dataset", {}).get("name", "dataset")


# ============================================================
# Command lines of the service scripts
# ============================================================

def quarantine(args: argparse.Namespace, command: str, *extra: str) -> list[str]:
    return [
        str(QUARANTINE), "--rules", str(args.rules),
        command, "--run", args.run, *extra,
    ]


def command_line(step: str, args: argparse.Namespace) -> list[str]:
    """Arguments for the service script of one step (without python)."""

    if step == "ingest":
        line = [str(INGEST), "--dataset", dataset_name(args.rules)]
        if args.input:
            line += ["--input", str(args.input)]
        return line

    if step == "profile":
        return [str(PROFILER), "--ingestion", args.run,
                "--dataset-name", dataset_name(args.rules)]

    if step == "validate":
        line = [str(ENGINE), "--ingestion", args.run, "--rules", str(args.rules)]
        return line + (["--force"] if getattr(args, "force", False) else [])

    if step == "export-template":
        extra = []
        if args.rule:
            extra += ["--rule", *args.rule]
        if args.error_code:
            extra += ["--error-code", *args.error_code]
        if args.output:
            extra += ["--output", str(args.output)]
        return quarantine(args, "template", *extra)

    if step == "import-corrections":
        extra = ["--file", str(args.file)]
        if args.user:
            extra += ["--user", args.user]
        return quarantine(args, "import", *extra)

    if step == "revalidate":
        extra = ["--max-attempts", str(args.max_attempts)]
        return quarantine(args, "revalidate", *extra, *(["--all"] if args.all else []))

    if step == "status":
        return quarantine(args, "status")

    if step == "merge":
        return [str(MERGE), "--run", args.run, "--rules", str(args.rules)]

    if step == "metrics":
        return [str(METRICS)]

    if step in ("metadata", "gold"):
        return [str(METADATA if step == "metadata" else GOLD), "--rules", str(args.rules)]

    if step == "import-files":
        return [str(IMPORT_FILES), "--rules", str(args.rules),
                "--data", str(args.data.resolve()),
                "--run", "all" if args.run == "latest" else args.run]

    raise ValueError(step)


def run(line: list[str]) -> int:
    script = Path(line[0]).relative_to(ROOT).as_posix()
    print("$ python " + " ".join([script, *line[1:]]), flush=True)
    return subprocess.run([sys.executable, *line], cwd=ROOT).returncode


def run_step(step: str, args: argparse.Namespace) -> int:
    return run(command_line(step, args))


def run_pipeline(args: argparse.Namespace) -> int:
    """ingest -> profile -> validate -> merge -> metrics -> metadata -> gold."""

    # Shown as it comes; the last line is the new ingestion id.
    line = ""
    print("$ python " + " ".join(command_line("ingest", args)), flush=True)
    with subprocess.Popen(
        [sys.executable, *command_line("ingest", args)],
        cwd=ROOT, stdout=subprocess.PIPE, text=True,
    ) as process:
        for line in process.stdout:
            print(line, end="", flush=True)
    if process.returncode:
        return process.returncode

    args.run = line.strip()
    for step in ("profile", "validate", "merge", "metrics", "metadata", "gold"):
        code = run_step(step, args)
        if code:
            return code
    print(f"Pipeline finished for {args.run}.")
    return 0


# ============================================================
# Arguments
# ============================================================

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--run", default="latest",
                        help="Ingestion id, or 'latest' (default).")
    shared.add_argument("--rules", type=Path, default=DEFAULT_RULES)

    parser = argparse.ArgumentParser(
        prog="dq.py", description="Run one data-quality step.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    steps = parser.add_subparsers(dest="step", required=True, metavar="step")

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        return steps.add_parser(name, parents=[shared], help=help_text)

    for name, help_text in (
        ("ingest", "Load a source file into bronze."),
        ("pipeline", "Ingest, profile, validate, merge, metrics, metadata and gold."),
    ):
        add(name, help_text).add_argument(
            "--input", type=Path, default=None,
            help="Source CSV (default: the ingestion service's).",
        )

    add("profile", "Profile an ingestion.")

    validate = add("validate", "Validate an ingestion.")
    validate.add_argument("--force", action="store_true",
                          help="Validate again, removing the run's previous "
                          "outputs and corrections.")

    template = add("export-template", "Write the correction template.")
    template.add_argument("--rule", nargs="*", default=None)
    template.add_argument("--error-code", nargs="*", default=None)
    template.add_argument("--output", type=Path, default=None)

    importer = add("import-corrections", "Import a filled-in template.")
    importer.add_argument("--file", type=Path, required=True)
    importer.add_argument("--user", default=None)

    revalidate = add("revalidate", "Apply corrections and validate again.")
    revalidate.add_argument("--max-attempts", type=int, default=3)
    revalidate.add_argument("--all", action="store_true")

    add("merge", "Merge valid and resolved records into silver.<dataset>.")
    add("metrics", "Update and check the DQ metric views (every run).")
    add("metadata", "Describe silver in the meta schema.")
    add("gold", "Build the gold star schema from the metadata.")
    add("import-files", "Load runs kept in data/ by earlier versions "
        "(default: every run).").add_argument(
        "--data", type=Path, default=DEFAULT_DATA,
        help="Folder holding validated/, quarantine/, corrected/, quality/.",
    )
    add("status", "Show quarantine counts.")
    add("ui", "Start the correction UI.")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_env(ROOT / ".env")
    args.rules = args.rules.resolve()

    if args.step == "pipeline":
        return run_pipeline(args)
    if args.step == "ui":
        return subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(UI), "--",
             "--rules", str(args.rules)],
            cwd=ROOT,
        ).returncode
    return run_step(args.step, args)


if __name__ == "__main__":
    sys.exit(main())
