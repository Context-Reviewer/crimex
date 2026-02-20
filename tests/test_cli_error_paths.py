from __future__ import annotations

import pytest

import crimex.cli as cli


def test_cli_unknown_command_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.sys, "argv", ["crimex", "definitely-not-a-command"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 2  # argparse error exit


def test_handle_fetch_unknown_source_exits_1(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text('{"source":"nope"}\n', encoding="utf-8")

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "fetch", "--spec", str(spec_path), "--out", str(tmp_path)],
    )
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1
