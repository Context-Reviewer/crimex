"""
Command-line interface for crimex.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from crimex import __version__
from crimex.bundle import BundleError, create_bundle
from crimex.connectors.bjs_ncvs import fetch_ncvs_data
from crimex.connectors.fbi_cde import fetch_fbi_data
from crimex.io import ensure_directory, read_json, read_jsonl
from crimex.manifest import generate_manifest
from crimex.normalize.common import normalize_all
from crimex.qa import validate_run_facts
from crimex.report.csv_out import write_facts_to_csv
from crimex.report.markdown import write_facts_to_markdown
from crimex.run import RunContext
from crimex.validate import validate_facts
from crimex.verify_run import verify_run


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = f"{_utc_now_iso()} {message}\n"
    log_path.open("a", encoding="utf-8").write(line)


def _dir_has_files(p: Path) -> bool:
    if not p.exists():
        return False
    return any(x.is_file() for x in p.rglob("*"))


def _read_spec_dict(spec_path: str) -> dict[str, Any]:
    obj = read_json(spec_path)
    if not isinstance(obj, dict):
        raise ValueError("Spec JSON must be an object (dictionary) at the top level")
    return cast(dict[str, Any], obj)


def main():
    parser = argparse.ArgumentParser(
        description="crimex: Deterministic crime data extraction and normalization tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # Fetch command (existing behavior preserved)
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from external sources")
    fetch_parser.add_argument("--spec", required=True, help="Path to query specification JSON file")
    fetch_parser.add_argument("--out", required=True, help="Output directory for raw data")
    fetch_parser.add_argument("--force", action="store_true", help="Force re-fetch even if cached")

    # Normalize command (existing behavior preserved)
    norm_parser = subparsers.add_parser("normalize", help="Normalize raw data to canonical facts")
    norm_parser.add_argument("--raw", required=True, help="Input directory containing raw data")
    norm_parser.add_argument("--out", required=True, help="Output file for normalized facts (JSONL)")

    # Report command (existing behavior preserved)
    report_parser = subparsers.add_parser("report", help="Generate reports from facts")
    report_parser.add_argument("--facts", required=True, help="Path to facts JSONL file")
    report_parser.add_argument("--out", required=True, help="Output directory for reports")
    report_parser.add_argument(
        "--explain",
        action="store_true",
        help="Include detailed explanation of data sources and units",
    )

    # Manifest command (existing behavior preserved)
    manifest_parser = subparsers.add_parser("manifest", help="Generate run manifest")
    manifest_parser.add_argument("--root", required=True, help="Root directory to scan for artifacts")
    manifest_parser.add_argument("--out", required=True, help="Output path for manifest JSON")

    # Validate command (existing behavior preserved)
    validate_parser = subparsers.add_parser("validate", help="Validate facts against schema")
    validate_parser.add_argument("--facts", required=True, help="Path to facts JSONL file")

    # Governed Run command (additive)
    run_parser = subparsers.add_parser(
        "run",
        help="Execute a governed run: fetch -> normalize -> report -> validate with artifact hashing",
    )
    run_parser.add_argument("--spec", required=True, help="Path to query specification JSON file")
    run_parser.add_argument(
        "--out-base",
        default="out/runs",
        help="Base directory for governed runs (default: out/runs)",
    )
    run_parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier (default: UTC timestamp YYYYMMDDTHHMMSSZ)",
    )
    run_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing run directory with the same run-id",
    )
    run_parser.add_argument("--force", action="store_true", help="Force re-fetch even if cached")
    run_parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip network fetch. Use existing raw data under the run directory (or fail if none).",
    )
    run_parser.add_argument(
        "--explain",
        action="store_true",
        help="Include explanation text in markdown report",
    )

    # verify-run command (existing behavior preserved)
    verify_parser = subparsers.add_parser(
        "verify-run",
        help="Verify a governed run directory against its run_manifest.json (SHA256 check)",
    )
    verify_parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to a run directory containing run_manifest.json",
    )

    # bundle command (additive)
    bundle_parser = subparsers.add_parser(
        "bundle",
        help="Export deterministic run bundle (run_bundle.zip) and register it in run_manifest.json",
    )
    bundle_parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to a run directory containing run_manifest.json",
    )
    bundle_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing run_bundle.zip if present",
    )

    # qa command (additive)
    qa_parser = subparsers.add_parser(
        "qa",
        help="Run deterministic QA validation on facts/facts.jsonl (read-only)",
    )
    qa_parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to a run directory containing facts/facts.jsonl",
    )

    # ui command (Phase 0.5)
    ui_parser = subparsers.add_parser(
        "ui",
        help="Launch local read-only UI for inspecting a run directory",
    )
    ui_parser.add_argument("--run-dir", required=True, help="Path to an existing run directory")
    ui_parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    ui_parser.add_argument("--port", type=int, default=0, help="Bind port (0 picks a free port)")

    args = parser.parse_args()

    if args.command == "fetch":
        handle_fetch(args)
    elif args.command == "normalize":
        handle_normalize(args)
    elif args.command == "report":
        handle_report(args)
    elif args.command == "manifest":
        handle_manifest(args)
    elif args.command == "validate":
        handle_validate(args)
    elif args.command == "run":
        handle_run(args)
    elif args.command == "verify-run":
        handle_verify_run(args)
    elif args.command == "bundle":
        handle_bundle(args)
    elif args.command == "qa":
        handle_qa(args)
    elif args.command == "ui":
        handle_ui(args)
    else:
        parser.print_help()
        sys.exit(1)


def handle_qa(args) -> None:
    run_dir = Path(args.run_dir)
    errors = validate_run_facts(run_dir)
    if errors:
        print("QA FAIL")
        for e in errors:
            print(e)
        sys.exit(1)
    print("QA PASS")
    sys.exit(0)


def handle_ui(args) -> None:
    from crimex.ui import server as ui_server

    argv = [
        "--run-dir",
        args.run_dir,
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    raise SystemExit(ui_server.main(argv))


def handle_bundle(args) -> None:
    run_dir = Path(args.run_dir)
    try:
        create_bundle(run_dir, force=bool(args.force))
    except BundleError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)
    sys.exit(0)


def handle_fetch(args):
    """Handles the fetch command."""
    spec_path = args.spec
    output_dir = args.out
    force = args.force

    print(f"Reading spec from {spec_path} ...")
    try:
        spec = _read_spec_dict(spec_path)
    except Exception as e:
        print(f"Error reading spec file: {e}", file=sys.stderr)
        sys.exit(1)

    source = spec.get("source")
    if not source:
        print("Error: Spec missing 'source' field", file=sys.stderr)
        sys.exit(1)

    ensure_directory(output_dir)

    if source == "fbi_cde":
        fetch_fbi_data(spec, output_dir, force)
    elif source == "bjs_ncvs":
        fetch_ncvs_data(spec, output_dir, force)
    else:
        print(f"Error: Unknown source '{source}'", file=sys.stderr)
        sys.exit(1)


def handle_normalize(args):
    """Handles the normalize command."""
    raw_dir = args.raw
    output_file = args.out

    try:
        normalize_all(raw_dir, output_file)
    except Exception as e:
        print(f"Error during normalization: {e}", file=sys.stderr)
        sys.exit(1)


def handle_report(args):
    """Handles the report command."""
    facts_path = args.facts
    output_dir = args.out
    explain = args.explain

    print(f"Reading facts from {facts_path} ...")
    try:
        facts = read_jsonl(facts_path)
    except Exception as e:
        print(f"Error reading facts file: {e}", file=sys.stderr)
        sys.exit(1)

    ensure_directory(output_dir)

    csv_file = os.path.join(output_dir, "report.csv")
    write_facts_to_csv(facts, csv_file)

    md_file = os.path.join(output_dir, "report.md")
    write_facts_to_markdown(facts, md_file, explain=explain)


def handle_manifest(args):
    """Handles the manifest command."""
    root_dir = args.root
    output_file = args.out

    command_str = " ".join(sys.argv)

    try:
        generate_manifest(root_dir, output_file, command_str)
    except Exception as e:
        print(f"Error generating manifest: {e}", file=sys.stderr)
        sys.exit(1)


def handle_validate(args):
    """Handles the validate command."""
    facts_path = args.facts
    validate_facts(facts_path)


def handle_verify_run(args):
    run_dir = Path(args.run_dir)
    result = verify_run(run_dir)

    if result.ok:
        print(f"OK: verified {result.checked} artifact(s) in {run_dir}")
        sys.exit(0)

    print(f"FAIL: verification failed for {run_dir}", file=sys.stderr)
    for err in result.errors:
        print(f" - {err}", file=sys.stderr)
    sys.exit(1)


def handle_run(args):
    """
    Governed run:
      - creates out/runs/<run_id>/...
      - fetches raw data into raw/<source>/ (unless --offline)
      - normalizes into facts/facts.jsonl
      - generates reports into reports/
      - validates facts
      - hashes artifacts and writes run_manifest.json
      - ALWAYS writes logs/run.log and run_manifest.json, even on failure
    """
    spec_path = args.spec
    out_base = Path(args.out_base)
    run_id = args.run_id
    overwrite = args.overwrite
    force = args.force
    offline = args.offline
    explain = args.explain

    print(f"Reading spec from {spec_path} ...")
    try:
        spec = _read_spec_dict(spec_path)
    except Exception as e:
        print(f"Error reading spec file: {e}", file=sys.stderr)
        sys.exit(1)

    source = spec.get("source")
    if not source:
        print("Error: Spec missing 'source' field", file=sys.stderr)
        sys.exit(1)

    try:
        run = RunContext(
            base_out=out_base,
            run_id=run_id,
            overwrite=overwrite,
            crimex_version=__version__,
        )
    except Exception as e:
        print(f"Error creating governed run directory: {e}", file=sys.stderr)
        sys.exit(1)

    log_path = run.logs_dir() / "run.log"
    _append_log(log_path, f"START run_id={run.run_id} version={__version__} source={source}")
    _append_log(
        log_path,
        f"spec_path={spec_path} out_base={out_base} overwrite={overwrite} force={force} offline={offline}",
    )

    raw_source_dir = run.raw_dir() / source
    facts_path = run.facts_dir() / "facts.jsonl"
    reports_dir = run.reports_dir()

    raw_source_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    def _finalize_and_exit(exit_code: int) -> None:
        try:
            run.register_tree(run.raw_dir())
        except Exception as e:
            _append_log(log_path, f"WARNING hashing raw failed: {type(e).__name__}: {e}")

        try:
            if facts_path.exists():
                run.register_artifact(facts_path)
        except Exception as e:
            _append_log(log_path, f"WARNING hashing facts failed: {type(e).__name__}: {e}")

        try:
            for rp in sorted(reports_dir.glob("*")):
                if rp.is_file():
                    run.register_artifact(rp)
        except Exception as e:
            _append_log(log_path, f"WARNING hashing reports failed: {type(e).__name__}: {e}")

        try:
            if log_path.exists():
                run.register_artifact(log_path)
        except Exception:
            pass

        try:
            run.write_manifest()
        except Exception as e:
            print(f"Error finalizing governed run: {e}", file=sys.stderr)
            sys.exit(2)

        print(f"Run complete: {run.path}")
        sys.exit(exit_code)

    if offline:
        _append_log(log_path, "OFFLINE mode enabled: skipping fetch.")
        if not _dir_has_files(raw_source_dir):
            _append_log(log_path, f"ERROR: offline mode requires existing raw data under {raw_source_dir}")
            print(
                f"Error: offline mode requires existing raw data under {raw_source_dir}",
                file=sys.stderr,
            )
            _finalize_and_exit(1)
    else:
        _append_log(log_path, "FETCH begin")
        try:
            if source == "fbi_cde":
                fetch_fbi_data(spec, str(raw_source_dir), force)
            elif source == "bjs_ncvs":
                fetch_ncvs_data(spec, str(raw_source_dir), force)
            else:
                _append_log(log_path, f"ERROR: unknown source '{source}'")
                print(f"Error: Unknown source '{source}'", file=sys.stderr)
                _finalize_and_exit(1)
        except BaseException as e:
            _append_log(log_path, f"ERROR during fetch: {type(e).__name__}: {e}")
            print(f"Error during fetch: {e}", file=sys.stderr)
            _finalize_and_exit(1)
        _append_log(log_path, "FETCH end")

    _append_log(log_path, "NORMALIZE begin")
    try:
        normalize_all(str(run.raw_dir()), str(facts_path))
    except BaseException as e:
        _append_log(log_path, f"ERROR during normalization: {type(e).__name__}: {e}")
        print(f"Error during normalization: {e}", file=sys.stderr)
        _finalize_and_exit(1)
    _append_log(log_path, "NORMALIZE end")

    _append_log(log_path, "REPORT begin")
    try:
        facts = read_jsonl(str(facts_path))

        csv_file = reports_dir / "report.csv"
        write_facts_to_csv(facts, str(csv_file))

        md_file = reports_dir / "report.md"
        write_facts_to_markdown(facts, str(md_file), explain=explain)
    except BaseException as e:
        _append_log(log_path, f"ERROR during report: {type(e).__name__}: {e}")
        print(f"Error during report generation: {e}", file=sys.stderr)
        _finalize_and_exit(1)
    _append_log(log_path, "REPORT end")

    _append_log(log_path, "VALIDATE begin")
    try:
        validate_facts(str(facts_path))
    except BaseException as e:
        _append_log(log_path, f"ERROR during validation: {type(e).__name__}: {e}")
        print(f"Error during validation: {e}", file=sys.stderr)
        _finalize_and_exit(1)
    _append_log(log_path, "VALIDATE end")

    _append_log(log_path, "SUCCESS")
    _finalize_and_exit(0)


if __name__ == "__main__":
    main()
