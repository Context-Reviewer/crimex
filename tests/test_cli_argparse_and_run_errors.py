from __future__ import annotations

import json
from pathlib import Path

import pytest

import crimex.cli as cli


def _write_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_no_args_exits_with_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Argparse error path (usually exits 2). Cheap coverage in cli.py near parser setup.
    """
    monkeypatch.setattr(cli.sys, "argv", ["crimex"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code in (2, 1)


def test_cli_unknown_command_exits_with_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Another argparse error path.
    """
    monkeypatch.setattr(cli.sys, "argv", ["crimex", "definitely-not-a-command"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code in (2, 1)


def test_cli_run_spec_missing_file_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hits handle_run early error branch for missing spec.
    """
    out_base = tmp_path / "out"
    out_base.mkdir(parents=True, exist_ok=True)

    missing_spec = tmp_path / "missing_spec.json"

    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "crimex",
            "run",
            "--spec",
            str(missing_spec),
            "--out-base",
            str(out_base),
            "--run-id",
            "RID",
            "--offline",
        ],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1


def test_cli_run_dir_exists_without_overwrite_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hits the 'run directory already exists' guarded branch (you saw this error already).
    """
    out_base = tmp_path / "out"
    run_id = "RID_EXISTS"
    run_dir = out_base / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    spec = tmp_path / "spec.json"
    _write_json(spec, {"source": "bjs_ncvs"})

    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "crimex",
            "run",
            "--spec",
            str(spec),
            "--out-base",
            str(out_base),
            "--run-id",
            run_id,
            "--offline",
            # intentionally NOT passing --overwrite
        ],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1
