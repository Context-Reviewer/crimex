from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class VerifyResult:
    ok: bool
    errors: list[str]
    checked: int


def verify_run(run_dir: Path) -> VerifyResult:
    run_dir = Path(run_dir)
    manifest_path = run_dir / "run_manifest.json"
    errors: list[str] = []

    if not manifest_path.exists():
        return VerifyResult(
            ok=False,
            errors=[f"Missing manifest: {manifest_path}"],
            checked=0,
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as e:
        return VerifyResult(
            ok=False,
            errors=[f"Failed to read/parse manifest: {manifest_path}: {type(e).__name__}: {e}"],
            checked=0,
        )

    artifacts: dict[str, str] = manifest.get("artifacts") or {}
    if not isinstance(artifacts, dict):
        return VerifyResult(
            ok=False,
            errors=[f"Invalid manifest: 'artifacts' must be an object/dict: {manifest_path}"],
            checked=0,
        )

    checked = 0
    for rel_path, expected_hash in sorted(artifacts.items(), key=lambda kv: kv[0]):
        checked += 1

        if not isinstance(rel_path, str) or not isinstance(expected_hash, str):
            errors.append(f"Invalid artifact entry types: {rel_path!r} -> {expected_hash!r}")
            continue

        # Manifest uses forward slashes; normalize to OS path safely
        artifact_path = run_dir / Path(rel_path)

        if not artifact_path.exists():
            errors.append(f"Missing artifact: {rel_path}")
            continue

        if not artifact_path.is_file():
            errors.append(f"Not a file artifact: {rel_path}")
            continue

        actual_hash = _sha256_file(artifact_path)
        if actual_hash != expected_hash:
            errors.append(f"Hash mismatch: {rel_path} expected={expected_hash} actual={actual_hash}")

    return VerifyResult(ok=(len(errors) == 0), errors=errors, checked=checked)
