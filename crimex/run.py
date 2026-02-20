from __future__ import annotations

import hashlib
import json
import platform
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    # Deterministic format (UTC), no colons for Windows path safety
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class RunContext:
    """
    Governed run context.

    - Creates a canonical run directory layout
    - Enforces overwrite rules
    - Registers artifacts for hashing
    - Writes a run_manifest.json with artifact SHA256s
    """

    base_out: Path
    run_id: str | None = None
    overwrite: bool = False
    crimex_version: str = "0.1.1"

    path: Path = field(init=False)
    _artifacts: dict[str, str] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.base_out = Path(self.base_out)
        self.base_out.mkdir(parents=True, exist_ok=True)

        if self.run_id is None:
            self.run_id = _default_run_id()

        self.path = self.base_out / self.run_id

        if self.path.exists():
            if not self.overwrite:
                raise FileExistsError(f"Run directory already exists: {self.path}. Use --overwrite to replace.")
            # True overwrite: wipe and recreate
            shutil.rmtree(self.path)

        self.path.mkdir(parents=True, exist_ok=True)

        # Canonical subdirs
        self.raw_dir().mkdir(parents=True, exist_ok=True)
        self.facts_dir().mkdir(parents=True, exist_ok=True)
        self.reports_dir().mkdir(parents=True, exist_ok=True)
        self.logs_dir().mkdir(parents=True, exist_ok=True)

    def raw_dir(self) -> Path:
        return self.path / "raw"

    def facts_dir(self) -> Path:
        return self.path / "facts"

    def reports_dir(self) -> Path:
        return self.path / "reports"

    def logs_dir(self) -> Path:
        return self.path / "logs"

    def register_artifact(self, file_path: Path) -> None:
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Artifact does not exist: {file_path}")

        rel = file_path.relative_to(self.path)
        # Normalize to forward slashes for manifest portability
        rel_key = str(rel).replace("\\", "/")
        self._artifacts[rel_key] = _sha256_file(file_path)

    def register_artifacts(self, file_paths: Iterable[Path]) -> None:
        for p in file_paths:
            self.register_artifact(p)

    def register_tree(self, root: Path) -> None:
        root = Path(root)
        if not root.exists():
            return
        for p in sorted(root.rglob("*")):
            if p.is_file():
                self.register_artifact(p)

    def write_manifest(self) -> Path:
        """
        Writes run_manifest.json and registers it as an artifact.
        Returns the manifest path.
        """
        manifest = {
            "run": {
                "run_id": self.run_id,
                "created_at": _utc_now_iso(),
                "python_version": sys.version,
                "platform": platform.platform(),
                "crimex_version": self.crimex_version,
            },
            "artifacts": dict(sorted(self._artifacts.items(), key=lambda kv: kv[0])),
        }

        manifest_path = self.path / "run_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        # NOTE: We intentionally do NOT require the manifest to include itself
        # in its own artifacts listing (self-referential hashing problem).
        return manifest_path
