import json
import sys
from pathlib import Path
from typing import Any

from crimex.io import load_text, read_json
from crimex.normalize.fbi_normalize import normalize_fbi_cde
from crimex.normalize.ncvs_normalize import normalize_ncvs
from crimex.schemas import Fact

NORMALIZERS = {
    "fbi_cde": normalize_fbi_cde,
    "bjs_ncvs": normalize_ncvs,
}


def normalize_raw_dir(raw_dir: str, output_file: str) -> None:
    raw_path = Path(raw_dir)
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    meta_files = sorted(raw_path.rglob("*.meta.json"))

    facts_out: list[dict[str, Any]] = []

    for meta_file in meta_files:
        try:
            meta_any = read_json(str(meta_file))
        except Exception as e:
            print(f"Error reading meta file {meta_file}: {e}", file=sys.stderr)
            continue
        if not isinstance(meta_any, dict):
            print(f"Error: meta file must be a JSON object: {meta_file}", file=sys.stderr)
            continue
        meta: dict[str, Any] = meta_any
    
        source = meta.get("source")
        if not source:
            print(f"Warning: Meta file missing source: {meta_file}", file=sys.stderr)
            continue

        # Derive raw file path from meta file name
        filename = meta_file.name.replace(".meta.json", ".json")
        raw_file = meta_file.with_name(filename)
        if not raw_file.exists():
            # Some sources may store raw as text; try .txt fallback
            txt_file = meta_file.with_name(meta_file.name.replace(".meta.json", ".txt"))
            if txt_file.exists():
                raw_file = txt_file
            else:
                print(f"Warning: Raw file not found for metadata {filename}", file=sys.stderr)
                continue

        # Read raw data
        try:
            raw_data = read_json(str(raw_file)) if raw_file.name.endswith(".json") else load_text(str(raw_file))
        except Exception as e:
            print(f"Error reading raw file {raw_file}: {e}", file=sys.stderr)
            continue

        # Normalize
        normalizer = NORMALIZERS.get(source)
        if not normalizer:
            print(f"Warning: No normalizer for source {source}", file=sys.stderr)
            continue

        try:
            facts = normalizer(raw_data, meta)

            # Validate facts using Pydantic
            validated = [Fact(**f).model_dump(mode="json") for f in facts]
            facts_out.extend(validated)
        except Exception as e:
            print(f"Error normalizing raw data for {source}: {e}", file=sys.stderr)
            continue

    # Deterministic sort
    facts_out.sort(key=lambda f: (f.get("source", ""), f.get("series", ""), f.get("geo", ""), f.get("period", "")))

    # Write JSONL
    with out_path.open("w", encoding="utf-8") as f:
        for fact in facts_out:
            f.write(json.dumps(fact, sort_keys=True) + "\n")
def normalize_all(raw_dir: str, output_file: str) -> None:
    normalize_raw_dir(raw_dir, output_file)
