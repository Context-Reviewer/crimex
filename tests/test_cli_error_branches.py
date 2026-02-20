from __future__ import annotations

import json
from pathlib import Path

import pytest

import crimex.cli as cli


def _write_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_fetch_unknown_source_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hit the 'unknown source' branch in handle_fetch (one of your big missing areas).
    """
    spec = tmp_path / "bad_spec.json"
    _write_json(spec, {"source": "not_a_real_source"})

    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "fetch", "--spec", str(spec), "--out", str(out_dir), "--force"],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1


def test_cli_report_missing_facts_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hit report handler error branch (facts file missing).
    """
    out_dir = tmp_path / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    missing_facts = tmp_path / "does_not_exist.jsonl"

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "report", "--facts", str(missing_facts), "--out", str(out_dir)],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1


def test_cli_verify_run_missing_manifest_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Another cheap branch: verify-run on a run dir that doesn't have run_manifest.json.
    """
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "verify-run", "--run-dir", str(run_dir)],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1
