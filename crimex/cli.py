"""
Command-line interface for crimex.
"""
import argparse
import sys
import os
import json
from crimex import __version__
from crimex.io import read_json, ensure_directory, read_jsonl, write_json
from crimex.connectors.fbi_cde import fetch_fbi_data
from crimex.connectors.bjs_ncvs import fetch_ncvs_data
from crimex.normalize.common import normalize_all
from crimex.report.csv_out import write_facts_to_csv
from crimex.report.markdown import write_facts_to_markdown
from crimex.manifest import generate_manifest
from crimex.validate import validate_facts

def main():
    parser = argparse.ArgumentParser(
        description="crimex: Deterministic crime data extraction and normalization tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    
    subparsers = parser.add_subparsers(dest="command", help="Subcommands")
    
    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from external sources")
    fetch_parser.add_argument("--spec", required=True, help="Path to query specification JSON file")
    fetch_parser.add_argument("--out", required=True, help="Output directory for raw data")
    fetch_parser.add_argument("--force", action="store_true", help="Force re-fetch even if cached")
    
    # Normalize command
    norm_parser = subparsers.add_parser("normalize", help="Normalize raw data to canonical facts")
    norm_parser.add_argument("--raw", required=True, help="Input directory containing raw data")
    norm_parser.add_argument("--out", required=True, help="Output file for normalized facts (JSONL)")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate reports from facts")
    report_parser.add_argument("--facts", required=True, help="Path to facts JSONL file")
    report_parser.add_argument("--out", required=True, help="Output directory for reports")
    report_parser.add_argument("--explain", action="store_true", help="Include detailed explanation of data sources and units")
    
    # Manifest command
    manifest_parser = subparsers.add_parser("manifest", help="Generate run manifest")
    manifest_parser.add_argument("--root", required=True, help="Root directory to scan for artifacts")
    manifest_parser.add_argument("--out", required=True, help="Output path for manifest JSON")
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate facts against schema")
    validate_parser.add_argument("--facts", required=True, help="Path to facts JSONL file")

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
    else:
        parser.print_help()
        sys.exit(1)

def handle_fetch(args):
    """Handles the fetch command."""
    spec_path = args.spec
    output_dir = args.out
    force = args.force
    
    print(f"Reading spec from {spec_path} ...")
    try:
        spec = read_json(spec_path)
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
    
    # CSV Report
    csv_file = os.path.join(output_dir, "report.csv")
    write_facts_to_csv(facts, csv_file)
    
    # Markdown Report
    md_file = os.path.join(output_dir, "report.md")
    write_facts_to_markdown(facts, md_file, explain=explain)

def handle_manifest(args):
    """Handles the manifest command."""
    root_dir = args.root
    output_file = args.out
    
    # Construct command string
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
