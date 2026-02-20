import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any

FIXED_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


class BundleError(Exception):
    pass


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _zipinfo_for_path(arc_path: str) -> zipfile.ZipInfo:
    zi = zipfile.ZipInfo(arc_path)
    zi.date_time = FIXED_ZIP_TIMESTAMP
    zi.compress_type = zipfile.ZIP_DEFLATED
    zi.create_system = 0
    zi.external_attr = 0
    zi.flag_bits = 0
    return zi


def _read_manifest_obj(run_dir: Path) -> dict[str, Any]:
    manifest_path = run_dir / "run_manifest.json"
    try:
        obj = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise BundleError(f"Failed to read run_manifest.json: {type(e).__name__}: {e}")

    if not isinstance(obj, dict):
        raise BundleError("Invalid governed manifest: root must be an object")

    artifacts = obj.get("artifacts")
    if not isinstance(artifacts, dict):
        raise BundleError("Invalid governed manifest: 'artifacts' must be a dict")

    return obj


def _serialize_manifest(obj: dict[str, Any]) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _update_governed_manifest_artifacts(run_dir: Path, bundle_sha256: str) -> None:
    manifest_path = run_dir / "run_manifest.json"
    obj = _read_manifest_obj(run_dir)
    obj["artifacts"]["run_bundle.zip"] = bundle_sha256
    manifest_path.write_text(
        json.dumps(obj, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _resolve_facts_path(run_dir: Path) -> Path:
    candidates_checked: list[str] = []

    p1 = run_dir / "facts" / "facts.jsonl"
    candidates_checked.append(str(p1))
    if p1.exists() and p1.is_file():
        return p1

    facts_dir = run_dir / "facts"
    if facts_dir.exists() and facts_dir.is_dir():
        jsonl_files = sorted([p for p in facts_dir.glob("*.jsonl") if p.is_file()])
        candidates_checked.append(str(facts_dir / "*.jsonl"))
        if len(jsonl_files) == 1:
            return jsonl_files[0]
        if len(jsonl_files) > 1:
            names = ", ".join(p.name for p in jsonl_files)
            raise BundleError(
                f"Ambiguous facts location: multiple .jsonl files under facts/: {names}. Expected exactly one."
            )

    p2 = run_dir / "facts.jsonl"
    candidates_checked.append(str(p2))
    if p2.exists() and p2.is_file():
        return p2

    # Fail loud with diagnostics
    facts_dir_state = ""
    if facts_dir.exists() and facts_dir.is_dir():
        all_children = sorted([p.name for p in facts_dir.iterdir()])
        facts_dir_state = f"facts/ exists with {len(all_children)} entries: {all_children}"
    else:
        facts_dir_state = "facts/ directory missing"

    raise BundleError("Facts file not found. Checked: " + "; ".join(candidates_checked) + f". {facts_dir_state}")


def _validate_run_structure(run_dir: Path, facts_path: Path) -> None:
    required_paths = [
        run_dir / "run_manifest.json",
        run_dir / "raw",
        run_dir / "reports",
        run_dir / "logs" / "run.log",
        facts_path,
    ]
    for p in required_paths:
        if not p.exists():
            raise BundleError(f"Required path missing: {p}")


def _collect_files_excluding_manifest_and_facts(run_dir: Path, facts_path: Path) -> list[Path]:
    files: list[Path] = []

    for base in ["raw", "reports"]:
        base_path = run_dir / base
        for p in sorted(base_path.rglob("*")):
            if p.is_file():
                files.append(p)

    files.append(run_dir / "logs" / "run.log")

    # Do not include the on-disk facts path directly; we will add it to the ZIP
    # under canonical archive name facts/facts.jsonl.
    # (facts_path may be facts.jsonl or some other name.)
    return sorted({p.resolve() for p in files})


def _relative_archive_path(run_dir: Path, file_path: Path) -> str:
    rel = file_path.relative_to(run_dir)
    return str(rel).replace("\\", "/")


def create_bundle(run_dir: Path, force: bool = False) -> Path:
    run_dir = Path(run_dir).resolve()
    if not run_dir.exists():
        raise BundleError("Run directory does not exist")

    facts_path = _resolve_facts_path(run_dir)
    _validate_run_structure(run_dir, facts_path)

    bundle_path = run_dir / "run_bundle.zip"
    if bundle_path.exists() and not force:
        raise BundleError("run_bundle.zip already exists (use --force to overwrite)")
    if bundle_path.exists():
        bundle_path.unlink()

    # Manifest snapshot inside bundle must not self-reference run_bundle.zip
    manifest_obj = _read_manifest_obj(run_dir)
    manifest_obj["artifacts"].pop("run_bundle.zip", None)
    manifest_bytes = _serialize_manifest(manifest_obj)

    other_files = _collect_files_excluding_manifest_and_facts(run_dir, facts_path)
    other_entries = sorted([_relative_archive_path(run_dir, f) for f in other_files])

    with zipfile.ZipFile(
        bundle_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as zf:
        # Always include manifest at canonical path
        zf.writestr(_zipinfo_for_path("run_manifest.json"), manifest_bytes)

        # Always include facts under canonical bundle path
        zf.writestr(_zipinfo_for_path("facts/facts.jsonl"), facts_path.read_bytes())

        for arc_path in other_entries:
            file_path = run_dir / arc_path
            zf.writestr(_zipinfo_for_path(arc_path), file_path.read_bytes())

    bundle_sha256 = _sha256_file(bundle_path)
    _update_governed_manifest_artifacts(run_dir, bundle_sha256)
    return bundle_path


def bundle_content_fingerprint(bundle_path: Path) -> str:
    h = hashlib.sha256()
    with zipfile.ZipFile(bundle_path, "r") as zf:
        infos = sorted(zf.infolist(), key=lambda i: i.filename)
        for info in infos:
            h.update(info.filename.encode("utf-8"))
            h.update(str(info.date_time).encode("utf-8"))
            h.update(str(info.CRC).encode("utf-8"))
            h.update(str(info.file_size).encode("utf-8"))
            h.update(zf.read(info.filename))
    return h.hexdigest()
