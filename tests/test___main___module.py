from __future__ import annotations

import runpy

import pytest


def test_module_main_version_exits_0(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    # Running as module should call cli.main()
    monkeypatch.setattr("sys.argv", ["crimex", "--version"])

    with pytest.raises(SystemExit) as e:
        runpy.run_module("crimex", run_name="__main__")

    assert e.value.code == 0
    out = capsys.readouterr().out
    assert out.strip()  # prints a version string
