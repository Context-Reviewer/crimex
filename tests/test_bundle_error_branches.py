from __future__ import annotations

from pathlib import Path

import pytest

from crimex.bundle import BundleError, create_bundle


def test_create_bundle_errors_when_run_dir_missing_required_files(tmp_path: Path) -> None:
    """
    Force an error path in create_bundle by providing a run_dir that is not a valid governed run
    (missing required artifacts like run_manifest.json, reports/, logs/, etc depending on implementation).
    """
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    with pytest.raises(BundleError):
        create_bundle(run_dir=run_dir, force=False)


def test_create_bundle_errors_when_bundle_exists_without_force(tmp_path: Path) -> None:
    """
    If create_bundle writes to the run_bundle.zip path, pre-create it so it must error.
    """
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    # Minimal governed run structure so we reach the "bundle exists" branch.
    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    (run_dir / "reports").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "facts").mkdir(parents=True, exist_ok=True)

    (run_dir / "logs" / "run.log").write_text("ok\n", encoding="utf-8")
    (run_dir / "facts" / "facts.jsonl").write_text("{}", encoding="utf-8")
    (run_dir / "run_manifest.json").write_text('{"run_id":"RID","artifacts":{}}', encoding="utf-8")

    bundle_path = run_dir / "run_bundle.zip"
    bundle_path.write_text("already exists", encoding="utf-8")

    with pytest.raises(BundleError) as e:
        create_bundle(run_dir=run_dir, force=False)
    assert "already exists" in str(e.value)
