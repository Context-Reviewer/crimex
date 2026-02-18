from pathlib import Path

import pytest

from crimex.run import RunContext


def test_run_directory_creation(tmp_path: Path) -> None:
    run = RunContext(base_out=tmp_path, run_id="test_run_001")
    assert run.path.exists()
    assert run.raw_dir().exists()
    assert run.facts_dir().exists()
    assert run.reports_dir().exists()
    assert run.logs_dir().exists()


def test_run_overwrite_protection(tmp_path: Path) -> None:
    RunContext(base_out=tmp_path, run_id="same_run")
    with pytest.raises(FileExistsError):
        RunContext(base_out=tmp_path, run_id="same_run", overwrite=False)


def test_run_overwrite_wipes_existing(tmp_path: Path) -> None:
    run1 = RunContext(base_out=tmp_path, run_id="wipe_run")
    marker = run1.path / "facts" / "marker.txt"
    marker.write_text("x", encoding="utf-8")
    assert marker.exists()

    run2 = RunContext(base_out=tmp_path, run_id="wipe_run", overwrite=True)
    assert run2.path.exists()
    assert not marker.exists()


def test_artifact_hashing_and_manifest(tmp_path: Path) -> None:
    run = RunContext(base_out=tmp_path, run_id="hash_test_001")

    facts = run.facts_dir() / "facts.jsonl"
    facts.write_text("hello\n", encoding="utf-8")

    run.register_artifact(facts)
    manifest_path = run.write_manifest()

    # Manifest file exists
    assert manifest_path.exists()

    # Manifest content includes the facts artifact
    text = manifest_path.read_text(encoding="utf-8")
    assert "facts/facts.jsonl" in text

    # NOTE: We intentionally do NOT require the manifest to list itself
    # as an artifact because self-hashing creates a self-referential
    # integrity problem unless done with a two-pass strategy.
