import json
import zipfile
from pathlib import Path

from crimex.bundle import create_bundle, bundle_content_fingerprint


def _create_minimal_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "raw").mkdir(parents=True)
    (run_dir / "facts").mkdir()
    (run_dir / "reports").mkdir()
    (run_dir / "logs").mkdir()

    (run_dir / "raw" / "a.json").write_text("{}", encoding="utf-8")
    (run_dir / "facts" / "facts.jsonl").write_text("", encoding="utf-8")
    (run_dir / "reports" / "r.json").write_text("{}", encoding="utf-8")
    (run_dir / "logs" / "run.log").write_text("", encoding="utf-8")

    manifest = {"run": {"run_id": "test"}, "artifacts": {}}
    (run_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return run_dir


def test_bundle_deterministic_semantics(tmp_path: Path) -> None:
    run_dir = _create_minimal_run(tmp_path)

    create_bundle(run_dir)
    first_fp = bundle_content_fingerprint(run_dir / "run_bundle.zip")

    create_bundle(run_dir, force=True)
    second_fp = bundle_content_fingerprint(run_dir / "run_bundle.zip")

    assert first_fp == second_fp


def test_bundle_updates_manifest_artifacts_map(tmp_path: Path) -> None:
    run_dir = _create_minimal_run(tmp_path)

    create_bundle(run_dir)

    manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert "artifacts" in manifest
    assert isinstance(manifest["artifacts"], dict)
    assert "run_bundle.zip" in manifest["artifacts"]
    assert isinstance(manifest["artifacts"]["run_bundle.zip"], str)
    assert len(manifest["artifacts"]["run_bundle.zip"]) == 64


def test_zip_has_fixed_timestamp(tmp_path: Path) -> None:
    run_dir = _create_minimal_run(tmp_path)
    create_bundle(run_dir)

    with zipfile.ZipFile(run_dir / "run_bundle.zip") as zf:
        infos = zf.infolist()
        assert len(infos) > 0
        for info in infos:
            assert info.date_time == (1980, 1, 1, 0, 0, 0)


def test_zip_manifest_does_not_self_reference_bundle(tmp_path: Path) -> None:
    run_dir = _create_minimal_run(tmp_path)
    create_bundle(run_dir)

    with zipfile.ZipFile(run_dir / "run_bundle.zip") as zf:
        manifest = json.loads(zf.read("run_manifest.json").decode("utf-8"))
    assert "artifacts" in manifest
    assert "run_bundle.zip" not in manifest["artifacts"]
