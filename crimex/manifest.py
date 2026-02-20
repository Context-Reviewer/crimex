import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_manifest(root_dir: str, out_path: str, command_str: str) -> dict[str, Any]:
    root_path = Path(root_dir)
    artifacts: dict[str, str] = {}

    # Walk the directory
    for root, _dirs, files in os.walk(root_dir):
        # Sort files to ensure deterministic order of traversal (at least within directory)
        # os.walk yields random order. We should sort `files` and `dirs` in place?
        files.sort()
        for fname in files:
            full = Path(root) / fname
            rel = full.relative_to(root_path).as_posix()
            artifacts[rel] = sha256_file(full)

    # Create manifest
    run_id = f"{datetime.now(timezone.utc).isoformat()}-{hashlib.sha256(command_str.encode()).hexdigest()[:8]}"

    manifest = {
        "run_id": run_id,
        "root": root_path.as_posix(),
        "command": command_str,
        "artifacts": artifacts,
    }

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
